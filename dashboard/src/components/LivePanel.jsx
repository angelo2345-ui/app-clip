import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
    Radio, Loader2, Square, RefreshCw, Download, FolderOpen, AlertTriangle,
    CheckCircle2, ArrowLeft,
} from 'lucide-react';
import { apiJson } from '../lib/api';
import { getApiUrl } from '../config';

// A live session is over once it is stopped, the broadcast ended, or it failed.
const TERMINAL = new Set(['stopped', 'ended', 'error']);

const POLL_MS = 5000;

const STATUS_TEXT = {
    starting: 'connecting to the broadcast…',
    recording: 'live · every window is clipped as it closes',
    stopping: 'stopping and handing over the last window…',
    stopped: 'stopped',
    ended: 'the broadcast ended',
    error: 'the session failed',
};

const fmtElapsed = (s) => {
    const t = Math.max(0, Math.floor(s || 0));
    return `${String(Math.floor(t / 60)).padStart(2, '0')}:${String(t % 60).padStart(2, '0')}`;
};

const errText = (e, fallback) => {
    const d = e?.detail;
    if (typeof d === 'string' && d) return d;
    if (d && typeof d.message === 'string') return d.message;
    return fallback;
};

export default function LivePanel({ sessionId, onClose, onOpenProject }) {
    const [session, setSession] = useState(null);
    const [error, setError] = useState('');
    const [stopping, setStopping] = useState(false);
    const [opening, setOpening] = useState(null);
    const pollRef = useRef(null);

    const load = useCallback(async () => {
        try {
            const d = await apiJson(`/api/live/${sessionId}`);
            setSession(d);
            setError('');
            return d;
        } catch (e) {
            setError(errText(e, 'Could not read the live session.'));
            return null;
        }
    }, [sessionId]);

    useEffect(() => { load(); }, [load]);

    // Poll while the broadcast is still being recorded; stop once it ends.
    useEffect(() => {
        clearInterval(pollRef.current);
        if (!session || TERMINAL.has(session.status)) return undefined;
        pollRef.current = setInterval(load, POLL_MS);
        return () => clearInterval(pollRef.current);
    }, [session, load]);

    const stop = useCallback(async () => {
        setStopping(true);
        setError('');
        try {
            const d = await apiJson(`/api/live/${sessionId}/stop`, { method: 'POST' });
            setSession(d);
        } catch (e) {
            setError(errText(e, 'Could not stop the session.'));
        } finally {
            setStopping(false);
        }
    }, [sessionId]);

    const openProject = useCallback(async (jobId) => {
        if (!onOpenProject || opening) return;
        setOpening(jobId);
        try { await onOpenProject(jobId); } catch { setError('Could not open this project yet.'); }
        setOpening(null);
    }, [onOpenProject, opening]);

    if (!session && !error) {
        return <div className="flex justify-center py-20"><Loader2 className="animate-spin text-brass" /></div>;
    }

    const status = session?.status || 'error';
    const recording = status === 'starting' || status === 'recording' || status === 'stopping';
    const clips = session?.clips || [];
    const jobs = (session?.jobs_detail || []).filter((j) => j.status === 'completed');

    return (
        <div className="max-w-5xl mx-auto animate-fade">
            <button onClick={onClose} className="btn-quiet text-xs mb-4">
                <ArrowLeft size={13} /> back to clip generator
            </button>

            <div className="flex items-start justify-between gap-4 mb-6">
                <div className="min-w-0">
                    <p className="eyebrow mb-1.5 flex items-center gap-2">
                        {recording
                            ? <span className="inline-flex items-center gap-1.5 text-danger"><span className="w-2 h-2 rounded-full bg-danger animate-pulse" /> LIVE</span>
                            : 'LIVE SESSION'}
                    </p>
                    <h1 className="font-display lowercase text-2xl sm:text-3xl text-ink mb-1 truncate">
                        {session?.title || 'live broadcast'}
                    </h1>
                    <p className="text-muted text-sm truncate">{session?.url}</p>
                </div>
                <div className="flex items-center gap-2 shrink-0">
                    <button onClick={load} className="btn-quiet text-xs" aria-label="refresh"><RefreshCw size={13} /></button>
                    {recording && (
                        <button onClick={stop} disabled={stopping} className="btn-ghost px-4 py-2 text-sm">
                            {stopping ? <Loader2 size={14} className="animate-spin" /> : <Square size={14} />}
                            stop session
                        </button>
                    )}
                </div>
            </div>

            {error && (
                <div className="mb-5 rounded-card border border-danger/40 bg-danger/5 px-4 py-3 text-sm text-danger flex items-center gap-2">
                    <AlertTriangle size={14} className="shrink-0" /> {error}
                </div>
            )}

            {/* Status line */}
            <div className="card p-4 sm:p-5 mb-6">
                <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm">
                    <span className={`inline-flex items-center gap-2 ${status === 'error' ? 'text-danger' : status === 'ended' ? 'text-warn' : 'text-ink2'}`}>
                        {recording
                            ? <Loader2 size={14} className="animate-spin text-brass" />
                            : status === 'error' ? <AlertTriangle size={14} />
                                : <CheckCircle2 size={14} className="text-ok" />}
                        {STATUS_TEXT[status] || status}
                    </span>
                    <span className="readout">elapsed {fmtElapsed(session?.elapsed)}</span>
                    <span className="readout">{session?.windows || 0} windows clipped</span>
                    {session?.failed ? <span className="readout text-warn">{session.failed} failed</span> : null}
                    <span className="readout">~{session?.window_seconds || 60}s window</span>
                </div>
                {session?.error && status !== 'error' && (
                    <p className="text-muted text-xs mt-2">{session.error}</p>
                )}
                {session?.error && status === 'error' && (
                    <p className="text-danger text-xs mt-2 whitespace-pre-line">{session.error}</p>
                )}
            </div>

            {/* Clips produced so far */}
            <div className="mb-6">
                <div className="flex items-center justify-between gap-3 mb-3">
                    <p className="eyebrow">CLIPS {clips.length ? `· ${clips.length}` : ''}</p>
                </div>
                {clips.length === 0 ? (
                    <div className="card p-8 text-center text-muted text-sm">
                        {recording
                            ? 'No clips yet. The first window closes about a minute in — keep this open.'
                            : 'This session produced no clips.'}
                    </div>
                ) : (
                    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
                        {clips.map((clip, i) => (
                            <div key={(clip.video_url || '') + i} className="card overflow-hidden flex flex-col">
                                <video
                                    src={getApiUrl(clip.video_url)}
                                    controls
                                    playsInline
                                    preload="metadata"
                                    className="w-full aspect-[9/16] object-contain bg-black"
                                />
                                <div className="p-2.5 flex flex-col gap-2 flex-1">
                                    <p className="text-xs text-ink2 line-clamp-2 leading-snug" title={clip.video_title_for_youtube_short}>
                                        {clip.video_title_for_youtube_short || `Clip ${i + 1}`}
                                    </p>
                                    <a
                                        href={getApiUrl(clip.video_url)}
                                        download
                                        className="mt-auto btn-quiet text-xs justify-center"
                                    >
                                        <Download size={13} /> download
                                    </a>
                                </div>
                            </div>
                        ))}
                    </div>
                )}
            </div>

            {/* Each window is its own project: open one to edit it fully. */}
            {jobs.length > 0 && (
                <div className="card p-4 sm:p-5 mb-10">
                    <p className="eyebrow mb-3">WINDOWS · open a project to edit its clips</p>
                    <div className="flex flex-wrap gap-2">
                        {jobs.map((j, i) => (
                            <button
                                key={j.job_id}
                                onClick={() => openProject(j.job_id)}
                                disabled={opening === j.job_id}
                                className="btn-quiet text-xs"
                            >
                                {opening === j.job_id ? <Loader2 size={13} className="animate-spin" /> : <FolderOpen size={13} />}
                                window {i + 1}
                            </button>
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
}
