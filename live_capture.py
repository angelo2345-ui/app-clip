"""Continuous capture of a live broadcast into fixed-length "windows".

A livestream never ends, so the whole-file pipeline cannot touch it: there is
no duration to probe, no end to wait for, and a single ``yt-dlp`` run would
never return. This module turns the ongoing HLS stream into a sequence of
self-contained MP4 files of ``window_seconds`` each, which the normal clip
pipeline can then process one by one as if they were ordinary uploads.

How it works (validated in the backend container against a real YouTube live):

    yt-dlp --hls-use-mpegts -f "bv*+ba/b" -o - <url>  |  ffmpeg -i pipe:0
        -c copy -f segment -segment_time 60 win_%05d.mp4

The stream is read once and re-muxed on the fly, so capture never pauses to
hand a window off, and every window is a complete, seekable MP4. Segments are
cut on keyframes (``-c copy``), so a window is approximately — not exactly —
``window_seconds`` long; that is fine for clip selection.

Nothing here imports FastAPI or the cloud package: it is a plain subprocess +
filesystem helper, so it is unit-testable against a temporary directory.
"""
import os
import re
import shutil
import signal
import subprocess
import sys
import time

# A window the pipeline gets billed and rendered from. 60 s holds any 15-60 s
# clip plus a little context on each side; shorter windows mean more jobs per
# hour and more overlap, longer ones delay the first clip.
DEFAULT_WINDOW_SECONDS = 60

# A window shorter than this is a partial tail (the muxer's last, unfinished
# segment); handing it to the pipeline would burn a job on a two-second video.
# Mirrors the app's MIN_SOURCE_SECONDS (default 45), which would reject a
# shorter source anyway.
MIN_WINDOW_SECONDS = 45

# How long ``probe_live`` waits for yt-dlp to answer before giving up. A live
# manifest is small, but YouTube can be slow to start one.
PROBE_TIMEOUT_SECONDS = 180

_WINDOW_RE = re.compile(r"^win_(\d{5,})\.mp4$")


def _ffmpeg_bin():
    # FFMPEG_BIN first for an explicit override, otherwise the bare name
    # "ffmpeg" and let the OS resolve it. We deliberately do NOT call
    # shutil.which(): in this repo's container the first hit on PATH is
    # ./bin/ffmpeg, a shell wrapper saved with CRLF line endings whose shebang
    # (#!/bin/sh\r) makes an explicit exec fail with ENOENT. execvp, given the
    # bare name, skips that broken entry and finds the real /usr/bin/ffmpeg —
    # which is exactly what every other module here calls.
    return os.environ.get("FFMPEG_BIN") or "ffmpeg"


def _ytdlp_cmd():
    # The same interpreter that runs the server, so it is the venv's yt-dlp and
    # not whatever ``yt-dlp`` happens to be first on PATH. ``-m`` is a module
    # lookup and needs no console script to be installed.
    return [sys.executable, "-m", "yt_dlp"]


def cookie_path():
    """The private cookie file for the first configured YouTube account, or
    None. Reused so a live capture is authenticated exactly like a download."""
    try:
        import yt_account
        session = yt_account.Session()
        return session.path or None
    except Exception:
        return None


def _common_args(cookie=None):
    """yt-dlp args shared by the probe and the recorder."""
    args = [
        "--no-warnings",
        "--no-progress",
        "--no-playlist",
        "--socket-timeout", "30",
        "--retries", "10",
        "--fragment-retries", "10",
        "--cache-dir", "false",
    ]
    if cookie:
        args += ["--cookies", cookie]
    return args


class NotLive(ValueError):
    """The URL is a single video but not a running broadcast."""


def probe_live(url, cookie=None, timeout=PROBE_TIMEOUT_SECONDS):
    """Ask yt-dlp about ``url``: is it live, and what is it.

    Returns ``{"is_live": bool, "id": str, "title": str, "was_live": bool}``.
    Raises ``NotASingleVideo`` (from yt_clients) when the URL is not one video
    and ``ValueError`` when yt-dlp could not read it at all.
    """
    from yt_clients import youtube_non_video_reason, NotASingleVideo
    reason = youtube_non_video_reason(url)
    if reason:
        raise NotASingleVideo(f"This link is {reason}.")

    cmd = _ytdlp_cmd() + _common_args(cookie or cookie_path()) + [
        "--print", "%(is_live)s\t%(was_live)s\t%(id)s\t%(title)s",
        "--skip-download", url,
    ]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise ValueError("Timed out asking YouTube about this link.")
    line = ""
    for candidate in (out.stdout or "").splitlines():
        if candidate.strip():
            line = candidate
            break
    if not line:
        detail = (out.stderr or "").strip().splitlines()
        raise ValueError(detail[-1] if detail else "Could not read this link.")
    parts = line.split("\t")
    is_live = parts[0].strip().lower() in ("true", "1")
    was_live = len(parts) > 1 and parts[1].strip().lower() in ("true", "1")
    vid = parts[2].strip() if len(parts) > 2 else ""
    title = parts[3].strip() if len(parts) > 3 else ""
    return {"is_live": is_live, "was_live": was_live, "id": vid, "title": title}


def probe_duration(path):
    """Duration of a media file in seconds via ffprobe, or None on any error."""
    ffprobe = os.environ.get("FFPROBE_BIN") or shutil.which("ffprobe") or "ffprobe"
    try:
        out = subprocess.run(
            [ffprobe, "-v", "error", "-show_entries", "format=duration",
             "-of", "default=nw=1:nk=1", path],
            capture_output=True, text=True, timeout=60)
        return float((out.stdout or "").strip())
    except (subprocess.SubprocessError, OSError, ValueError):
        return None


def window_index(path):
    """The numeric index of a window file, or None if it is not one."""
    m = _WINDOW_RE.match(os.path.basename(path))
    return int(m.group(1)) if m else None


def list_windows(work_dir):
    """Every window file in ``work_dir``, ordered by index."""
    found = []
    try:
        names = os.listdir(work_dir)
    except OSError:
        return []
    for name in names:
        idx = window_index(name)
        if idx is not None:
            found.append((idx, os.path.join(work_dir, name)))
    found.sort()
    return [path for _, path in found]


class LiveRecorder:
    """Records ``url`` into ``win_00000.mp4``, ``win_00001.mp4``, ... in
    ``work_dir`` until :meth:`stop`.

    The newest file may still be growing; :meth:`completed_windows` returns
    every window the muxer has already closed, which is the set safe to hand
    to the pipeline.
    """

    def __init__(self, url, work_dir, window_seconds=DEFAULT_WINDOW_SECONDS,
                 cookie=None):
        self.url = url
        self.work_dir = work_dir
        self.window_seconds = int(window_seconds)
        self.cookie = cookie
        self._ytdlp = None
        self._ffmpeg = None
        self._yt_log = None
        self._ff_log = None
        self.started_at = None
        self.stopped = False

    # -- process wiring -------------------------------------------------- #
    def start(self):
        os.makedirs(self.work_dir, exist_ok=True)
        self._yt_log = open(os.path.join(self.work_dir, "yt-dlp.log"), "wb")
        self._ff_log = open(os.path.join(self.work_dir, "ffmpeg.log"), "wb")

        yt_args = _ytdlp_cmd() + _common_args(self.cookie or cookie_path()) + [
            "--hls-use-mpegts",
            "-f", "bv*+ba/b",
            "-o", "-",
            self.url,
        ]
        # Own process group: a stop can take the recorder down without touching
        # the server that spawned it.
        self._ytdlp = subprocess.Popen(
            yt_args, stdout=subprocess.PIPE, stderr=self._yt_log,
            start_new_session=True)
        ff_args = [
            _ffmpeg_bin(), "-hide_banner", "-loglevel", "warning", "-nostdin",
            "-i", "pipe:0",
            "-c", "copy",
            "-f", "segment",
            "-segment_time", str(self.window_seconds),
            "-reset_timestamps", "1",
            "-segment_format", "mp4",
            os.path.join(self.work_dir, "win_%05d.mp4"),
        ]
        try:
            self._ffmpeg = subprocess.Popen(
                ff_args, stdin=self._ytdlp.stdout, stderr=self._ff_log,
                start_new_session=True)
        except FileNotFoundError:
            self._ytdlp.terminate()
            raise RuntimeError("ffmpeg is not installed: live capture needs it.")
        # The parent must not hold the read end, or ffmpeg never sees EOF when
        # yt-dlp exits.
        self._ytdlp.stdout.close()
        self.started_at = time.time()

    def is_alive(self):
        """True while both halves are still running."""
        return (self._ytdlp is not None and self._ytdlp.poll() is None
                and self._ffmpeg is not None and self._ffmpeg.poll() is None)

    def ytdlp_exit_code(self):
        return None if self._ytdlp is None else self._ytdlp.poll()

    def stop(self, timeout=15):
        """Stop the capture, letting the muxer close the last window."""
        self.stopped = True
        for proc in (self._ytdlp, self._ffmpeg):
            if proc is None or proc.poll() is not None:
                continue
            try:
                proc.terminate()
            except OSError:
                pass
        deadline = time.time() + timeout
        for proc in (self._ffmpeg, self._ytdlp):
            if proc is None:
                continue
            remaining = max(0.0, deadline - time.time())
            try:
                proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except (OSError, ProcessLookupError):
                    try:
                        proc.kill()
                    except OSError:
                        pass
        for fh in (self._yt_log, self._ff_log):
            try:
                if fh:
                    fh.close()
            except OSError:
                pass

    # -- windows --------------------------------------------------------- #
    def completed_windows(self):
        """Windows the muxer has closed, oldest first.

        The list is the files ffmpeg has already finished writing: every window
        except the newest one it may still be appending to. Once capture has
        stopped, the last file is complete too.
        """
        windows = list_windows(self.work_dir)
        if not windows:
            return []
        if not self.stopped and self.is_alive():
            windows = windows[:-1]  # the newest is still being written
        return windows

    def last_log_lines(self, n=8):
        """The tail of the ffmpeg / yt-dlp logs, for an error message."""
        chunks = []
        for name in ("yt-dlp.log", "ffmpeg.log"):
            path = os.path.join(self.work_dir, name)
            try:
                with open(path, "rb") as fh:
                    text = fh.read().decode("utf-8", "replace").strip()
            except OSError:
                continue
            if text:
                chunks.append(f"{name}:\n" + "\n".join(text.splitlines()[-n:]))
        return "\n".join(chunks)
