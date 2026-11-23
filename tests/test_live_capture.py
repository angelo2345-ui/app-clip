"""Continuous capture of a live broadcast: parsing, probing and window bookkeeping.

Nothing here spawns yt-dlp or ffmpeg: the module is a plain subprocess + files
helper, so every branch is exercised against fakes and a temporary directory.
"""
import os
import subprocess
import types

import pytest

import live_capture

URL = "https://www.youtube.com/watch?v=abc123"


@pytest.fixture
def no_non_video(monkeypatch):
    """Let a plain watch URL through yt_clients' non-video allowlist check."""
    import yt_clients
    monkeypatch.setattr(yt_clients, "youtube_non_video_reason", lambda u: None)


# --- window file names --------------------------------------------------------

def test_window_index_reads_the_padded_number():
    assert live_capture.window_index("win_00000.mp4") == 0
    assert live_capture.window_index("/tmp/x/win_12345.mp4") == 12345
    # Fewer than five digits is not a window the muxer wrote.
    assert live_capture.window_index("win_123.mp4") is None
    assert live_capture.window_index("other.mp4") is None


def test_list_windows_is_ordered_and_ignores_other_files(tmp_path):
    for n in (0, 2, 1):
        (tmp_path / f"win_{n:05d}.mp4").write_bytes(b"")
    (tmp_path / "ffmpeg.log").write_text("noise")
    got = [os.path.basename(p) for p in live_capture.list_windows(str(tmp_path))]
    assert got == ["win_00000.mp4", "win_00001.mp4", "win_00002.mp4"]


def test_list_windows_on_a_missing_directory_is_empty():
    assert live_capture.list_windows("/nonexistent/live/dir") == []


# --- probe_live ---------------------------------------------------------------

def test_probe_live_reads_is_live_id_and_title(monkeypatch, no_non_video):
    def fake_run(cmd, **kw):
        assert "--skip-download" in cmd
        assert "%(is_live)s\t%(was_live)s\t%(id)s\t%(title)s" in cmd
        return types.SimpleNamespace(stdout="True\tFalse\tabc123\tMy Stream\n", stderr="", returncode=0)

    monkeypatch.setattr(live_capture.subprocess, "run", fake_run)
    info = live_capture.probe_live(URL, cookie="")
    assert info == {"is_live": True, "was_live": False, "id": "abc123", "title": "My Stream"}


def test_probe_live_reports_a_finished_broadcast(monkeypatch, no_non_video):
    def fake_run(cmd, **kw):
        return types.SimpleNamespace(stdout="False\tTrue\tabc123\tOld Stream\n", stderr="", returncode=0)

    monkeypatch.setattr(live_capture.subprocess, "run", fake_run)
    info = live_capture.probe_live(URL, cookie="")
    assert info["is_live"] is False and info["was_live"] is True


def test_probe_live_refuses_a_non_video_url(monkeypatch):
    import yt_clients
    monkeypatch.setattr(yt_clients, "youtube_non_video_reason", lambda u: "a playlist")
    with pytest.raises(yt_clients.NotASingleVideo):
        live_capture.probe_live(URL)


def test_probe_live_times_out(monkeypatch, no_non_video):
    def fake_run(cmd, **kw):
        raise subprocess.TimeoutExpired(cmd, 1)

    monkeypatch.setattr(live_capture.subprocess, "run", fake_run)
    with pytest.raises(ValueError):
        live_capture.probe_live(URL)


def test_probe_live_uses_the_last_stderr_line_when_it_cannot_read(monkeypatch, no_non_video):
    def fake_run(cmd, **kw):
        return types.SimpleNamespace(stdout="", stderr="ERROR: bad\nERROR: nope\n", returncode=1)

    monkeypatch.setattr(live_capture.subprocess, "run", fake_run)
    with pytest.raises(ValueError, match="nope"):
        live_capture.probe_live(URL)


# --- probe_duration -----------------------------------------------------------

def test_probe_duration_reads_ffprobe(monkeypatch):
    monkeypatch.setattr(live_capture.subprocess, "run",
                        lambda cmd, **kw: types.SimpleNamespace(stdout="12.5\n"))
    assert live_capture.probe_duration("x.mp4") == 12.5


def test_probe_duration_never_raises(monkeypatch):
    def boom(cmd, **kw):
        raise OSError("ffprobe missing")

    monkeypatch.setattr(live_capture.subprocess, "run", boom)
    assert live_capture.probe_duration("x.mp4") is None


# --- LiveRecorder window bookkeeping ------------------------------------------

class _Proc:
    """A subprocess stand-in for LiveRecorder.stop()."""

    def __init__(self):
        self.pid = os.getpid()
        self._rc = None
        self.terminated = False

    def poll(self):
        return self._rc

    def terminate(self):
        self.terminated = True
        self._rc = 0

    def wait(self, timeout=None):
        self._rc = 0
        return 0

    def kill(self):
        self._rc = -9


def _recorder(tmp_path, monkeypatch, n=3, alive=True, stopped=False):
    rec = live_capture.LiveRecorder(URL, str(tmp_path))
    for i in range(n):
        (tmp_path / f"win_{i:05d}.mp4").write_bytes(b"")
    monkeypatch.setattr(rec, "is_alive", lambda: alive)
    rec.stopped = stopped
    return rec


def test_completed_windows_holds_back_the_one_still_growing(tmp_path, monkeypatch):
    rec = _recorder(tmp_path, monkeypatch, n=3, alive=True, stopped=False)
    got = [os.path.basename(p) for p in rec.completed_windows()]
    assert got == ["win_00000.mp4", "win_00001.mp4"]


def test_completed_windows_returns_all_once_stopped(tmp_path, monkeypatch):
    rec = _recorder(tmp_path, monkeypatch, n=3, alive=False, stopped=True)
    assert len(rec.completed_windows()) == 3


def test_completed_windows_returns_all_when_capture_died(tmp_path, monkeypatch):
    # The recorder exited on its own: the muxer closed its last file, so nothing
    # is held back even though stop() was never called.
    rec = _recorder(tmp_path, monkeypatch, n=3, alive=False, stopped=False)
    assert len(rec.completed_windows()) == 3


def test_stop_terminates_both_halves_and_closes_logs(tmp_path):
    rec = live_capture.LiveRecorder(URL, str(tmp_path))
    rec._yt_log = open(tmp_path / "yt.log", "wb")
    rec._ff_log = open(tmp_path / "ff.log", "wb")
    rec._ytdlp = _Proc()
    rec._ffmpeg = _Proc()
    rec.stop()
    assert rec.stopped is True
    assert rec._ytdlp.terminated and rec._ffmpeg.terminated
    assert rec._yt_log.closed and rec._ff_log.closed


# --- shared yt-dlp args -------------------------------------------------------

def test_common_args_carry_the_cookie_file_only_when_given():
    plain = live_capture._common_args(None)
    assert "--no-playlist" in plain and "--cookies" not in plain
    authed = live_capture._common_args("/tmp/ck.txt")
    assert "--cookies" in authed and "/tmp/ck.txt" in authed
