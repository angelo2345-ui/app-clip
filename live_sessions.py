"""Live sessions: clip a broadcast while it is still going.

A live session is a long-lived background task. It records the ongoing stream
into fixed-length windows (:mod:`live_capture`) and pushes each finished
window through the normal clip pipeline — ``POST /api/process`` in-process,
exactly like Autopilot — so metering, the per-plan job limit, the quality gate
and the download proxies keep applying unchanged. Every window is billed and
rendered like any other 1-minute source, and its clips land next to the job's.

The session stops when asked (``stop_session``) or when the broadcast itself
ends; it never clips the same window twice. It holds no user session of its
own: the auth headers of whoever started it are replayed on each submission,
so the jobs belong to that user in cloud mode and run unauthenticated in
self-host.
"""
import asyncio
import os
import shutil
import time
import uuid

import live_capture

# How often the runner looks for a freshly closed window. Small next to the
# window itself, so a window is handed on within a few seconds of closing.
TICK_SECONDS = 5

# A window shorter than this is a partial tail, not a clip source.
MIN_WINDOW_SECONDS = live_capture.MIN_WINDOW_SECONDS

# Give up re-submitting a window after this many failures (a 429 storm, a bad
# window), so one stuck window cannot pin the loop forever.
MAX_SUBMIT_ATTEMPTS = 3

_sessions = {}
_app = None


def configure(app):
    """Hand the module the FastAPI app it submits windows to (mirrors the way
    ``cloud/autopilot`` receives its app), avoiding an import cycle."""
    global _app
    _app = app


class LiveSession:
    def __init__(self, sid, url, work_dir, window_seconds, headers, owner, title):
        self.id = sid
        self.url = url
        self.work_dir = work_dir
        self.window_seconds = int(window_seconds)
        self.headers = dict(headers or {})
        self.owner = None if owner is None else str(owner)
        self.title = title
        self.status = "starting"          # starting|recording|stopping|stopped|ended|error
        self.error = None
        self.started_at = time.time()
        self.stopped_at = None
        self.job_ids = []
        self.processed = set()            # window indices submitted
        self.attempts = {}                # window index -> failed submissions
        self.failed = 0
        self.recorder = None
        self.task = None

    @property
    def windows_done(self):
        return len(self.processed)

    def view(self):
        return {
            "id": self.id,
            "url": self.url,
            "title": self.title,
            "status": self.status,
            "error": self.error,
            "window_seconds": self.window_seconds,
            "windows": self.windows_done,
            "failed": self.failed,
            "jobs": list(self.job_ids),
            "started_at": self.started_at,
            "elapsed": round((self.stopped_at or time.time()) - self.started_at, 1),
        }


async def start_session(url, work_root, window_seconds=live_capture.DEFAULT_WINDOW_SECONDS,
                        headers=None, owner=None):
    """Validate that ``url`` is a running broadcast and start recording it.

    Raises ``NotASingleVideo`` / ``NotLive`` / ``ValueError`` before anything is
    written, so a bad link never leaves a half-started session behind.
    """
    window_seconds = int(window_seconds or live_capture.DEFAULT_WINDOW_SECONDS)
    if window_seconds < 30:
        window_seconds = 30
    info = await asyncio.to_thread(live_capture.probe_live, url)
    if not info.get("is_live"):
        raise live_capture.NotLive(
            "That link is not a running broadcast. Paste the link of a live video.")

    sid = uuid.uuid4().hex[:12]
    work_dir = os.path.join(work_root, sid)
    session = LiveSession(sid, url, work_dir, window_seconds, headers, owner,
                          info.get("title"))
    recorder = live_capture.LiveRecorder(url, work_dir, window_seconds=window_seconds)
    try:
        await asyncio.to_thread(recorder.start)
    except Exception:
        shutil.rmtree(work_dir, ignore_errors=True)
        raise
    session.recorder = recorder
    _sessions[sid] = session
    session.task = asyncio.create_task(_run(session))
    print(f"🔴 Live session {sid} started: {url} (window={window_seconds}s)")
    return session


def get_session(sid):
    return _sessions.get(sid)


def list_sessions(owner=None):
    out = list(_sessions.values())
    if owner is not None:
        out = [s for s in out if s.owner == str(owner)]
    out.sort(key=lambda s: s.started_at, reverse=True)
    return out


async def stop_session(sid):
    """Stop recording. The last window is handed to the pipeline and the
    session ends; safe to call twice."""
    session = _sessions.get(sid)
    if session is None:
        return None
    if session.status not in ("starting", "recording"):
        return session
    session.status = "stopping"
    if session.recorder is not None:
        await asyncio.to_thread(session.recorder.stop)
    await _drain(session)
    session.status = "stopped"
    session.stopped_at = time.time()
    if session.task is not None and not session.task.done():
        session.task.cancel()
    print(f"🔴 Live session {sid} stopped ({session.windows_done} windows).")
    return session


async def shutdown():
    """Stop every recorder (server shutdown): leave no orphan processes."""
    for session in list(_sessions.values()):
        if session.status in ("starting", "recording"):
            try:
                await stop_session(session.id)
            except Exception:
                pass


# --------------------------------------------------------------------------- #
# The runner
# --------------------------------------------------------------------------- #
async def _run(session):
    session.status = "recording"
    rec = session.recorder
    grace = 0
    try:
        while session.status == "recording":
            await asyncio.sleep(TICK_SECONDS)
            alive = rec.is_alive()
            if alive:
                grace = 0
            else:
                grace += 1
            await _process_new_windows(session)
            if not alive:
                # Let the muxer close its last file and hand it over, then end.
                if grace >= 2:
                    await _drain(session)
                    session.status = "ended"
                    if rec.ytdlp_exit_code() not in (0, None):
                        session.status = "error"
                        session.error = ("The broadcast ended or could not be "
                                         "recorded.\n" + rec.last_log_lines())
                    else:
                        session.error = "The broadcast ended."
                    break
    except asyncio.CancelledError:
        raise
    except Exception as e:  # never let a session die silently
        session.status = "error"
        session.error = f"{type(e).__name__}: {e}"
        print(f"⚠️  Live session {session.id} failed: {session.error}")
    finally:
        session.stopped_at = session.stopped_at or time.time()


async def _drain(session):
    """Hand over every window still waiting, once capture has stopped."""
    await _process_new_windows(session, final=True)


async def _process_new_windows(session, final=False):
    rec = session.recorder
    if rec is None:
        return
    for path in rec.completed_windows():
        idx = live_capture.window_index(path)
        if idx is None or idx in session.processed:
            continue
        dur = await asyncio.to_thread(live_capture.probe_duration, path)
        if dur is not None and dur < MIN_WINDOW_SECONDS:
            # A partial tail (the muxer's last, unfinished segment): nothing to
            # clip there. Drop it and move on.
            session.processed.add(idx)
            _unlink(path)
            continue
        job_id = await _submit_window(session, path)
        if job_id:
            session.processed.add(idx)
            session.job_ids.append(job_id)
            _unlink(path)
        else:
            session.attempts[idx] = session.attempts.get(idx, 0) + 1
            session.failed += 1
            if session.attempts[idx] >= MAX_SUBMIT_ATTEMPTS:
                session.processed.add(idx)
                _unlink(path)


async def _submit_window(session, path):
    """Push one window through the normal pipeline; returns its job id or None."""
    if _app is None:
        print("⚠️  Live sessions are not configured with the app; cannot submit.")
        return None
    import httpx
    base = os.environ.get("PUBLIC_API_URL", "").rstrip("/") or "http://localhost"
    headers = {"User-Agent": "OpenShorts-Live/1.0", **session.headers}
    try:
        with open(path, "rb") as fh:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=_app),
                                         base_url=base, timeout=900) as client:
                resp = await client.post(
                    "/api/process",
                    data={"acknowledged": "true", "auto_hook": "1"},
                    files={"file": (os.path.basename(path), fh, "video/mp4")},
                    headers=headers)
        try:
            payload = resp.json()
        except ValueError:
            payload = {}
        if resp.status_code == 200 and payload.get("job_id"):
            print(f"🔴 Live {session.id} window {os.path.basename(path)} -> "
                  f"job {payload['job_id']}")
            return payload["job_id"]
        detail = payload.get("detail") if isinstance(payload, dict) else None
        print(f"⚠️  Live {session.id} window {os.path.basename(path)} refused "
              f"({resp.status_code}{': ' + str(detail) if detail else ''})")
    except Exception as e:
        print(f"⚠️  Live {session.id} submit error for {os.path.basename(path)}: {e}")
    return None


def _unlink(path):
    try:
        os.remove(path)
    except OSError:
        pass
