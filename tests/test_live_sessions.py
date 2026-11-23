"""Live sessions: turning closed windows into jobs, once each, with retries.

The recorder and the pipeline call are both faked: these tests cover the
bookkeeping (dedupe, short-window drop, retry cap, stop/idempotency) that keeps
one stuck window from pinning the session or double-billing a user.
"""
import asyncio
import os

import pytest

import live_capture
import live_sessions

URL = "https://www.youtube.com/watch?v=abc123"


@pytest.fixture(autouse=True)
def clean_state():
    live_sessions._sessions.clear()
    live_sessions._app = None
    yield
    live_sessions._sessions.clear()
    live_sessions._app = None


class FakeRecorder:
    """Stands in for LiveRecorder. The test decides which windows are 'closed'."""

    def __init__(self, windows=(), alive=True, exit_code=0):
        self._windows = list(windows)
        self._alive = alive
        self._exit_code = exit_code
        self.stopped = False
        self.started = False

    def start(self):
        self.started = True

    def is_alive(self):
        return self._alive

    def ytdlp_exit_code(self):
        return self._exit_code

    def stop(self, timeout=15):
        self.stopped = True

    def completed_windows(self):
        return list(self._windows)

    def last_log_lines(self, n=8):
        return "ffmpeg.log:\nsome failure"


def _win(tmp_path, i):
    path = os.path.join(str(tmp_path), f"win_{i:05d}.mp4")
    with open(path, "wb") as fh:
        fh.write(b"x")
    return path


def _session(tmp_path, windows=(), alive=True, exit_code=0):
    s = live_sessions.LiveSession("sid", URL, str(tmp_path), 60, {}, "user1", "Stream")
    s.recorder = FakeRecorder(windows, alive=alive, exit_code=exit_code)
    return s


# --- _process_new_windows -----------------------------------------------------

def test_each_closed_window_becomes_one_job_and_is_deleted(tmp_path, monkeypatch):
    s = _session(tmp_path)
    w0, w1 = _win(tmp_path, 0), _win(tmp_path, 1)
    s.recorder._windows = [w0, w1]
    monkeypatch.setattr(live_capture, "probe_duration", lambda p: 60.0)
    submitted = []

    async def fake_submit(session, path):
        submitted.append(path)
        return f"job-{len(submitted)}"

    monkeypatch.setattr(live_sessions, "_submit_window", fake_submit)
    asyncio.run(live_sessions._process_new_windows(s))
    assert s.processed == {0, 1}
    assert s.job_ids == ["job-1", "job-2"]
    assert not os.path.exists(w0) and not os.path.exists(w1)
    # A second pass finds nothing new: no window is clipped twice.
    asyncio.run(live_sessions._process_new_windows(s))
    assert len(submitted) == 2


def test_a_partial_tail_is_dropped_without_a_job(tmp_path, monkeypatch):
    s = _session(tmp_path)
    w = _win(tmp_path, 0)
    s.recorder._windows = [w]
    monkeypatch.setattr(live_capture, "probe_duration", lambda p: 10.0)

    async def must_not_submit(session, path):
        raise AssertionError("a short window must not be submitted")

    monkeypatch.setattr(live_sessions, "_submit_window", must_not_submit)
    asyncio.run(live_sessions._process_new_windows(s))
    assert s.processed == {0} and s.job_ids == [] and s.failed == 0
    assert not os.path.exists(w)


def test_a_failing_window_is_retried_then_given_up(tmp_path, monkeypatch):
    s = _session(tmp_path)
    w = _win(tmp_path, 0)
    s.recorder._windows = [w]
    monkeypatch.setattr(live_capture, "probe_duration", lambda p: 60.0)

    async def refuse(session, path):
        return None

    monkeypatch.setattr(live_sessions, "_submit_window", refuse)
    for _ in range(live_sessions.MAX_SUBMIT_ATTEMPTS):
        asyncio.run(live_sessions._process_new_windows(s))
    assert s.failed == live_sessions.MAX_SUBMIT_ATTEMPTS
    assert s.processed == {0}          # given up, so the loop lets it go
    assert not os.path.exists(w)


def test_a_window_that_fails_once_is_still_clipped_on_the_retry(tmp_path, monkeypatch):
    s = _session(tmp_path)
    w = _win(tmp_path, 0)
    s.recorder._windows = [w]
    monkeypatch.setattr(live_capture, "probe_duration", lambda p: 60.0)
    calls = []

    async def flaky(session, path):
        calls.append(path)
        return None if len(calls) == 1 else "job-1"

    monkeypatch.setattr(live_sessions, "_submit_window", flaky)
    asyncio.run(live_sessions._process_new_windows(s))     # fails
    asyncio.run(live_sessions._process_new_windows(s))     # succeeds
    assert s.job_ids == ["job-1"] and s.processed == {0}
    assert not os.path.exists(w)


# --- _submit_window -----------------------------------------------------------

def test_submit_window_posts_the_window_to_the_pipeline(tmp_path, monkeypatch):
    import httpx
    live_sessions._app = object()
    w = _win(tmp_path, 0)
    seen = {}

    class FakeResp:
        status_code = 200

        def json(self):
            return {"job_id": "job-9"}

    class FakeClient:
        def __init__(self, **kw):
            seen["transport"] = kw.get("transport")

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, path, data=None, files=None, headers=None):
            seen.update(path=path, data=data, headers=headers, files=files)
            return FakeResp()

    monkeypatch.setattr(httpx, "AsyncClient", FakeClient)
    s = _session(tmp_path)
    job = asyncio.run(live_sessions._submit_window(s, w))
    assert job == "job-9"
    assert seen["path"] == "/api/process"
    assert seen["data"]["acknowledged"] == "true"
    assert seen["headers"]["User-Agent"] == "OpenShorts-Live/1.0"
    assert seen["files"]["file"][0] == "win_00000.mp4"


def test_submit_window_replays_the_owners_headers(tmp_path, monkeypatch):
    import httpx
    live_sessions._app = object()
    w = _win(tmp_path, 0)
    seen = {}

    class FakeResp:
        status_code = 200

        def json(self):
            return {"job_id": "job-1"}

    class FakeClient:
        def __init__(self, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, path, data=None, files=None, headers=None):
            seen["headers"] = headers
            return FakeResp()

    monkeypatch.setattr(httpx, "AsyncClient", FakeClient)
    s = _session(tmp_path)
    s.headers = {"Authorization": "Bearer x", "Cookie": "SID=y"}
    asyncio.run(live_sessions._submit_window(s, w))
    assert seen["headers"]["Authorization"] == "Bearer x"
    assert seen["headers"]["Cookie"] == "SID=y"


def test_submit_window_is_a_no_op_without_the_app(tmp_path):
    live_sessions._app = None
    w = _win(tmp_path, 0)
    assert asyncio.run(live_sessions._submit_window(_session(tmp_path), w)) is None


def test_submit_window_returns_none_when_the_pipeline_refuses(tmp_path, monkeypatch):
    import httpx
    live_sessions._app = object()
    w = _win(tmp_path, 0)

    class FakeResp:
        status_code = 429

        def json(self):
            return {"detail": "too many jobs"}

    class FakeClient:
        def __init__(self, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, path, data=None, files=None, headers=None):
            return FakeResp()

    monkeypatch.setattr(httpx, "AsyncClient", FakeClient)
    assert asyncio.run(live_sessions._submit_window(_session(tmp_path), w)) is None


# --- start / stop -------------------------------------------------------------

def _patch_recorder(monkeypatch, recorder=None):
    holder = {"recorder": recorder}

    def factory(url, work_dir, window_seconds=60, cookie=None):
        holder["args"] = (url, work_dir, window_seconds)
        return holder["recorder"]

    monkeypatch.setattr(live_capture, "LiveRecorder", factory)
    return holder


def test_start_session_refuses_a_link_that_is_not_live(tmp_path, monkeypatch):
    monkeypatch.setattr(live_capture, "probe_live", lambda url, **kw: {"is_live": False})
    with pytest.raises(live_capture.NotLive):
        asyncio.run(live_sessions.start_session(URL, str(tmp_path)))
    assert live_sessions.list_sessions() == []


def test_start_session_records_a_running_broadcast(tmp_path, monkeypatch):
    monkeypatch.setattr(live_capture, "probe_live",
                        lambda url, **kw: {"is_live": True, "title": "Stream T"})
    holder = _patch_recorder(monkeypatch, FakeRecorder())

    async def noop_run(session):
        return

    monkeypatch.setattr(live_sessions, "_run", noop_run)

    async def go():
        s = await live_sessions.start_session(URL, str(tmp_path), window_seconds=60, owner="u1")
        await asyncio.sleep(0)                    # let the (no-op) runner finish
        return s

    s = asyncio.run(go())
    assert s.title == "Stream T" and s.status == "starting"
    assert live_sessions.get_session(s.id) is s
    assert live_sessions.list_sessions("u1") == [s]
    assert live_sessions.list_sessions("someone-else") == []
    assert holder["args"][1].endswith(s.id)       # work dir namespaced by session


def test_start_session_cleans_up_when_recording_cannot_start(tmp_path, monkeypatch):
    monkeypatch.setattr(live_capture, "probe_live", lambda url, **kw: {"is_live": True})

    class Boom(FakeRecorder):
        def start(self):
            raise RuntimeError("ffmpeg is not installed")

    _patch_recorder(monkeypatch, Boom())
    with pytest.raises(RuntimeError):
        asyncio.run(live_sessions.start_session(URL, str(tmp_path)))
    assert live_sessions.list_sessions() == []
    # Nothing half-started is left on disk.
    assert os.listdir(str(tmp_path)) == []


def test_stop_session_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setattr(live_capture, "probe_live", lambda url, **kw: {"is_live": True})
    rec = FakeRecorder()
    _patch_recorder(monkeypatch, rec)

    async def noop_run(session):
        return

    monkeypatch.setattr(live_sessions, "_run", noop_run)

    async def go():
        s = await live_sessions.start_session(URL, str(tmp_path), owner="u1")
        await live_sessions.stop_session(s.id)
        first = s.status
        await live_sessions.stop_session(s.id)     # a second call must not drop windows
        return s, first

    s, first = asyncio.run(go())
    assert first == "stopped" and s.status == "stopped"
    assert s.stopped_at is not None


def test_stop_session_on_an_unknown_id_is_harmless():
    assert asyncio.run(live_sessions.stop_session("nope")) is None


# --- the runner ---------------------------------------------------------------

def test_runner_ends_when_the_recorder_dies(tmp_path, monkeypatch):
    monkeypatch.setattr(live_sessions, "TICK_SECONDS", 0)
    monkeypatch.setattr(live_capture, "probe_duration", lambda p: 60.0)
    s = _session(tmp_path, alive=False, exit_code=0)
    asyncio.run(live_sessions._run(s))
    assert s.status == "ended"
    assert s.error == "The broadcast ended."


def test_runner_reports_an_error_when_capture_failed(tmp_path, monkeypatch):
    monkeypatch.setattr(live_sessions, "TICK_SECONDS", 0)
    monkeypatch.setattr(live_capture, "probe_duration", lambda p: 60.0)
    s = _session(tmp_path, alive=False, exit_code=1)
    asyncio.run(live_sessions._run(s))
    assert s.status == "error"
    assert "could not be recorded" in s.error


def test_shutdown_stops_recording_sessions(tmp_path, monkeypatch):
    monkeypatch.setattr(live_capture, "probe_live", lambda url, **kw: {"is_live": True})
    rec = FakeRecorder()
    _patch_recorder(monkeypatch, rec)

    async def noop_run(session):
        return

    monkeypatch.setattr(live_sessions, "_run", noop_run)

    async def go():
        s = await live_sessions.start_session(URL, str(tmp_path), owner="u1")
        s.status = "recording"
        await live_sessions.shutdown()
        return s

    s = asyncio.run(go())
    assert s.status == "stopped" and rec.stopped is True
