"""`pmwallets watch`: follow the account's fill stream and hand every fill on — to stdout, or to a command."""

from __future__ import annotations

import asyncio
import contextlib
import datetime as _dt
import inspect
import json
import math
import os
import re
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional, Union

from .client import AsyncClient
from .market import MarketLookup, micro_to_decimal
from .stream import FileStateStore, FillMeta, FillStream
from .tools import frame_market
from .types import Fill

QUEUE = "queue.jsonl"
LOCK = "watch.lock."  # generation locks `watch.lock.<n>`: the highest n is the lock; see _lock()
HANDED = "handed.txt"  # eventIds already handed to the command, one per line: the at-most-once record
HANDED_KEEP = 20_000

OutFn = Callable[[str], Union[None, Awaitable[None]]]
LogFn = Callable[[dict[str, Any]], None]


def iso(seconds: float) -> str:
    """UTC ISO time with milliseconds and Z, as JavaScript's toISOString() writes it."""
    ms = int(seconds * 1000)
    return _dt.datetime.fromtimestamp(ms // 1000, _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + f".{ms % 1000:03d}Z"


class Watcher:
    """Follows the account's fill stream and hands every fill on — to stdout, or to a command (an AI agent, a script).

    Without `exec`, every fill is written to `out` (default stdout) as one JSON line. With `exec`, the shell command
    runs once per batch of fills: the batch arrives on its stdin as JSON lines, one fill per line (the same lines are
    in the file named by PMW_BATCH_FILE; PMW_BATCH_SIZE is the count).

    Handing a fill to a command is AT MOST ONCE. Before the command starts, the batch's eventIds are appended to
    `handed.txt` and synced to disk; a fill whose eventId is there is never queued or handed again — not when the
    stream replays it because the process died before its cursor was saved, not when a batch is recovered. A
    `running-*` file found at start-up (the process died while the command ran) is moved to `failed/`, never re-run:
    the command may already have acted on it (placed an order). A batch whose command exits non-zero or times out
    goes to `failed/` too. A fill is synced into the queue before the stream cursor moves past it, so nothing between
    the stream and the command is lost across a restart.

    The directory holds the stream cursor (stream.json, same format as the Node package), the queue and the lock; the
    two packages' watchers can take turns on one directory.

    `batch_sec`: how long to wait after the first queued fill before running the command, so fills that arrive close
    together go as one batch. `max_batch`: most fills in one batch; the rest go in the next. `exec_timeout_sec`: a
    command still running after this is killed and its batch counted as failed. `markets=None`: no market lookups.
    `log` receives progress and problems, one dict per event (the CLI prints them to stderr as JSON lines).
    `stream_options` are passed to FillStream (tests: a fake connector).
    """

    def __init__(self, client: AsyncClient, dir: Union[str, Path], exec: Optional[str] = None, batch_sec: float = 0.0,
                 max_batch: int = 50, exec_timeout_sec: float = 600.0, takeover: bool = False,
                 markets: Optional[MarketLookup] = None, out: Optional[OutFn] = None, log: Optional[LogFn] = None,
                 stream_options: Optional[dict[str, Any]] = None, now: Callable[[], float] = time.time) -> None:
        if isinstance(max_batch, bool) or not isinstance(max_batch, int) or max_batch < 1:
            raise ValueError("max_batch must be a whole number ≥ 1")
        self.client = client
        self.dir = Path(dir)
        self.exec = exec
        self.batch_sec = batch_sec
        self.max_batch = max_batch
        self.exec_timeout_sec = exec_timeout_sec
        self.takeover = takeover
        self.markets = markets
        self.out = out
        self.log: LogFn = log or (lambda e: None)
        self.stream_options = stream_options or {}
        self.now = now
        self.stream: Optional[FillStream] = None
        self._timer: Optional[asyncio.TimerHandle] = None
        self._child: Optional[asyncio.Task[None]] = None
        # a start() in progress: stop() waits for it, so no step of it touches the directory after the lock is gone
        self._starting: Optional[asyncio.Task[None]] = None
        self._proc_pid: Optional[int] = None  # the command running now: stop() ends its whole process tree
        # pending SIGKILLs for the running command: cancelled when it is over, or they could hit a reused process id
        self._escalations: set[asyncio.TimerHandle] = set()
        self._running = False
        self._lock_file: Optional[Path] = None  # the lock file this process created, if it holds the directory
        self._handed: dict[str, None] = {}  # insertion-ordered set: the oldest go first when the record is trimmed
        self._handed_lines = 0
        self._handed_limit = HANDED_KEEP * 2
        # something may be queued: _kick() runs on every fill, and must not read the whole queue to find out
        self._pending = True
        self._done: Optional[asyncio.Future[int]] = None
        self._bg: set[asyncio.Task[Any]] = set()  # the loop keeps only weak references to tasks

    def _path(self, name: str) -> Path:
        return self.dir / name

    async def start(self) -> None:
        task = asyncio.ensure_future(self._start_now())
        self._starting = task
        try:
            await task
        finally:
            if self._starting is task:
                self._starting = None

    async def _start_now(self) -> None:
        self._done = asyncio.get_running_loop().create_future()
        (self.dir / "failed").mkdir(parents=True, exist_ok=True)
        self._lock()
        self._running = True
        try:
            # a command a dead watcher left running is stopped whatever this run's mode: it may still be trading
            await self._stop_orphans()
            if not self._running:  # stopped meanwhile: touch nothing more in the directory
                raise _StoppedWhileStarting()
            if self.exec:
                self._load_handed()
                self._recover()
            else:
                # fills queued for a command by an earlier --exec run are past the stream cursor: printing from the
                # stream would silently skip them
                waiting = len(_read_lines(self._path(QUEUE))) + sum(1 for p in self.dir.iterdir() if p.name.startswith("batch-"))
                if waiting:
                    raise RuntimeError(f"{self.dir} holds fills queued for a command by an earlier --exec run: start with --exec to hand them over, or move {self._path(QUEUE)} (and any batch-* files) away to drop them")
            if not self._running:
                raise _StoppedWhileStarting()
            self.stream = FillStream(self.client, self._on_fill, on_event=self._on_event,
                                     store=FileStateStore(self._path("stream.json")), takeover=self.takeover,
                                     **self.stream_options)
            await self.stream.start()
            if not self._running:
                raise _StoppedWhileStarting()
        except BaseException:
            # a start that fails (or is stopped midway) must not leave the directory locked, or a stream running
            stopped_midway = not self._running
            self._running = False
            if self.stream is not None:
                with contextlib.suppress(Exception):
                    await self.stream.stop()
            self.stream = None
            self._unlock()
            if stopped_midway:
                if self._done is not None and not self._done.done():
                    self._done.set_result(0)
                return
            raise
        if self.exec:
            self._kick()

    async def wait(self) -> int:
        """Block until the watcher has stopped (fatal stream error, or stop()); returns the exit code."""
        if self._done is None:
            raise RuntimeError("start() first")
        return await asyncio.shield(self._done)

    def _spawn(self, coro: Awaitable[Any]) -> None:
        t = asyncio.ensure_future(coro)
        self._bg.add(t)
        t.add_done_callback(self._bg.discard)

    async def stop(self, code: int = 0) -> None:
        if not self._running:
            return
        self._running = False
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        # the command first: stopping the stream can take long (a replay in progress), and a command asked to stop
        # must not go on trading meanwhile. Its batch goes to failed/; it is stopped, not waited for.
        if self._proc_pid is not None:
            pid = self._proc_pid
            _kill_tree(pid, signal.SIGTERM)
            # a command that ignores SIGTERM must not hold the directory (and maybe go on trading) until its timeout
            self._escalate(pid)
        # a start still in progress sees the stop at its next step and cleans up after itself (not when stop() is
        # called from inside that start: it would wait for itself)
        starting = self._starting
        if starting is not None and starting is not asyncio.current_task():
            with contextlib.suppress(BaseException):
                await asyncio.shield(starting)
        if self.stream is not None:
            await self.stream.stop()
        # the lock is held until the running batch is over: released earlier, a new watcher would take its file and
        # start a second command next to it
        if self._child is not None:
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await asyncio.shield(self._child)
        self._unlock()
        if self._done is not None and not self._done.done():
            self._done.set_result(code)

    def _on_event(self, e: dict[str, Any]) -> None:
        t = e.get("type")
        if t == "fatal":
            self.log({"event": "fatal", "error": _message(e.get("error"))})
            self._spawn(self.stop(2))
        elif t == "error":
            self.log({"event": "stream_error", "error": _message(e.get("error"))})
        elif t != "connecting":
            self.log({"event": t, **{k: v for k, v in e.items() if k != "type"}})

    async def _on_fill(self, fill: Fill, meta: FillMeta) -> None:
        market = await self.markets.by_token(fill["tokenId"]) if self.markets else None
        rec = {
            **fill,
            "source": meta.source,
            "receivedAt": iso(self.now()),
            "usdcAmount": micro_to_decimal(fill.get("usdc")),
            "sharesAmount": micro_to_decimal(fill.get("shares")),
            "feeAmount": micro_to_decimal(fill.get("fee")),
            "market": market or frame_market(fill),
        }
        line = json.dumps(rec, separators=(",", ":"), ensure_ascii=False)
        if not self.exec:
            r = (self.out or _write_stdout)(line)
            # any awaitable (a coroutine, a Future, a Task): the fill counts as delivered only once it is done
            if inspect.isawaitable(r):
                await r
            return
        # a replay of a fill the command already had (the cursor was not saved before a crash) must not reach it again
        if fill["eventId"] in self._handed:
            self.log({"event": "skipped_already_handed", "eventId": fill["eventId"]})
            return
        # synchronous and synced (no await before it returns): the cursor moves only after on_fill returns, so a
        # queued fill is on disk before it counts as delivered
        _append_synced(self._path(QUEUE), line + "\n")
        self._pending = True
        self._kick()

    def _kick(self) -> None:
        """Run the command for the next batch, once the batch window has passed and no command is running."""
        if not self._running or self._child is not None or self._timer is not None or not self._pending:
            return
        self._timer = asyncio.get_running_loop().call_later(self.batch_sec, self._fire)

    def _fire(self) -> None:
        self._timer = None
        self._child = asyncio.get_running_loop().create_task(self._dispatch())
        self._child.add_done_callback(self._dispatched)

    def _dispatched(self, task: asyncio.Task[None]) -> None:
        self._child = None
        if task.cancelled():
            return
        e = task.exception()
        if e is not None:
            # the queue files could not be read or moved (disk full, permissions): stop instead of guessing
            self.log({"event": "fatal", "error": f"batch dispatch failed: {_message(e)}"})
            self._spawn(self.stop(1))
            return
        self._kick()

    def _queued(self) -> list[str]:
        """The queue, de-duplicated, without anything already handed to the command."""
        return [x for x in _dedupe(_read_lines(self._path(QUEUE))) if _event_id_of(x) not in self._handed]

    def _repair_tail(self, name: str) -> None:
        """A crash in the middle of an append can leave a last line without its newline. That fill was never confirmed
        (the stream cursor did not move past it, so the stream sends it again), but the next append would be glued onto
        the fragment and lost with it: cut the fragment off first."""
        path = self._path(name)
        try:
            data = path.read_bytes()
        except FileNotFoundError:
            return
        if not data or data.endswith(b"\n"):
            return
        keep = data[: data.rfind(b"\n") + 1]
        _write_synced(path, keep)
        self.log({"event": "partial_line_removed", "file": str(path), "bytes": len(data) - len(keep)})

    def _load_handed(self) -> None:
        self._repair_tail(HANDED)
        self._repair_tail(QUEUE)
        ids = _read_lines(self._path(HANDED))
        self._handed = dict.fromkeys(ids)
        self._handed_lines = len(ids)

    def _mark_handed(self, ids: list[str]) -> None:
        """Record a batch as handed over — synced to disk BEFORE the command can see it."""
        for i in ids:
            self._handed[i] = None
        self._handed_lines += len(ids)
        if self._handed_lines > self._handed_limit:
            # forget only what the stream can never replay: fills at or before its SAVED cursor (a replay starts
            # strictly after it). During a long replay the cursor is saved only at the end, so everything after it is kept.
            cursor = self._saved_cursor()
            keep = [i for i in self._handed if cursor is None or (at := _position_of(i)) is None or at > cursor]
            _write_synced(self._path(HANDED), "\n".join(keep) + "\n")
            self._handed = dict.fromkeys(keep)
            self._handed_lines = len(keep)
            self._handed_limit = max(HANDED_KEEP * 2, len(keep) * 2)
        else:
            _append_synced(self._path(HANDED), "\n".join(ids) + "\n")

    def _saved_cursor(self) -> Optional[tuple[int, int]]:
        """(block, logIndex) the stream has saved in stream.json, if any — synced first: the stream writes it without
        fsync, and handed ids are forgotten on its word. A sync that fails counts as no cursor (everything is kept)."""
        try:
            # read-write: Windows flushes (FlushFileBuffers) only a handle opened for writing; nothing is written
            fd = os.open(self._path("stream.json"), os.O_RDWR | _O_BINARY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            _sync_dir(self.dir)
        except OSError:
            return None
        try:
            c = json.loads(self._path("stream.json").read_bytes())
        except (OSError, ValueError):
            return None
        if not isinstance(c, dict):
            return None
        b, li = c.get("block"), c.get("logIndex")
        return (b, li) if _is_int(b) and _is_int(li) else None

    async def _dispatch(self) -> None:
        lines = self._queued()
        if not lines:
            self._pending = False
            return
        if not self._running:
            return
        batch, rest = lines[: self.max_batch], lines[self.max_batch:]
        bid = f"{iso(self.now()).replace(':', '-').replace('.', '-')}-{os.getpid()}"
        pending = self._path(f"batch-{bid}.jsonl")
        running_file = self._path(f"running-{bid}.jsonl")
        # batch-* = taken from the queue but not started (put back at start-up); running-* = the command has it
        _write_synced(pending, "\n".join(batch) + "\n")
        _write_synced(self._path(QUEUE), "\n".join(rest) + "\n" if rest else "")
        self._pending = bool(rest)
        event_ids = [_event_id_of(x) for x in batch]
        self._mark_handed(event_ids)
        os.replace(pending, running_file)
        _sync_dir(self.dir)

        started = self.now()
        # `queued`: what is still waiting after this batch — a backlog that keeps growing means the command is too slow
        self.log({"event": "batch_start", "fills": len(batch), "queued": len(rest), "file": str(running_file)})
        ok, reason = await self._run_command("\n".join(batch) + "\n", running_file, len(batch))
        ms = round((self.now() - started) * 1000)
        if ok:
            running_file.unlink()
            self.log({"event": "batch_done", "fills": len(batch), "ms": ms})
        else:
            failed = self.dir / "failed" / f"{bid}.jsonl"
            os.replace(running_file, failed)
            self.log({"event": "batch_failed", "fills": len(batch), "ms": ms, "reason": reason, "file": str(failed), "eventIds": event_ids})

    async def _run_command(self, data: str, file: Path, size: int) -> tuple[bool, Optional[str]]:
        # absolute: it is also how a later start recognises this command's processes (see _stop_orphans)
        env = {**os.environ, "PMW_BATCH_FILE": os.path.abspath(file), "PMW_BATCH_SIZE": str(size)}
        loop = asyncio.get_running_loop()
        exited: asyncio.Future[None] = loop.create_future()

        class _Exit(asyncio.SubprocessProtocol):
            # The batch is over when the command EXITS. asyncio's Process.wait() returns only once every pipe is also
            # closed (Python >= 3.12), and a grandchild holding the inherited stdin would keep the batch open.
            def process_exited(self) -> None:
                if not exited.done():
                    exited.set_result(None)

        try:
            # stdout / stderr inherited: what the command prints goes where the watcher's own output goes.
            # Its own session / process group (POSIX): an agent CLI starts children of its own, and a timeout or a
            # stop must end all of them, not only the shell — a surviving agent could go on trading.
            transport, _ = await loop.subprocess_shell(_Exit, self.exec or "", stdin=subprocess.PIPE, stdout=None, stderr=None,
                                                       env=env, start_new_session=sys.platform != "win32")
        except Exception as e:
            return False, f"could not start the command: {_message(e)}"
        pid = transport.get_pid()
        self._proc_pid = pid
        # who is running this batch, for the next start if this process dies with the command still running
        pid_file = file.with_name((file.name[: -len(".jsonl")] if file.name.endswith(".jsonl") else file.name) + ".pid")
        try:
            _write_synced(pid_file, json.dumps({"pid": pid, "host": socket.gethostname(), "batchFile": os.path.abspath(file)}, separators=(",", ":")))
        except Exception as e:
            # unrecorded, a command could outlive a crash of this process unseen by the next start: do not let it run
            # (it has not been given the batch yet)
            self._proc_pid = None
            self._kill_batch(pid, file)
            transport.close()
            return False, f"could not record the command's process id ({_message(e)}); stopped it"
        # stop() came while the process was being started (it saw no pid): the full stop path for it here — no batch
        # on its stdin, SIGTERM now, SIGKILL in 5 s, and the batch fails once it has exited
        stopped_at_start = not self._running
        if stopped_at_start:
            _kill_tree(pid, signal.SIGTERM)
            self._escalate(pid)
        else:
            stdin = transport.get_pipe_transport(0)
            # buffered and flushed by the loop, never blocking; a command that never reads stdin (it uses
            # PMW_BATCH_FILE) closes the pipe: that is not a failure
            with contextlib.suppress(Exception):
                if stdin is not None:
                    stdin.write(data.encode("utf-8"))
                    stdin.close()
        timed_out = False
        try:
            try:
                await asyncio.wait_for(asyncio.shield(exited), self.exec_timeout_sec)
            except asyncio.TimeoutError:
                timed_out = True
                _kill_tree(pid, signal.SIGTERM)
                try:
                    await asyncio.wait_for(asyncio.shield(exited), 5.0)
                except asyncio.TimeoutError:
                    _kill_tree(pid, getattr(signal, "SIGKILL", signal.SIGTERM))
                    await exited
        finally:
            for t in self._escalations:
                t.cancel()
            self._escalations.clear()
            self._proc_pid = None
            with contextlib.suppress(OSError):
                pid_file.unlink()
            # the batch is over when the command exits: anything it left running is stopped with it — its process
            # group, and on Linux any process that left the group but still carries this batch in its environment
            self._kill_batch(pid, file)
            code = transport.get_returncode()
            transport.close()
        sig = _signal_name(code)
        if timed_out:
            return False, f"timed out after {math.floor(self.exec_timeout_sec + 0.5)} s"
        if stopped_at_start or (not self._running and code != 0):
            return False, f"stopped with the watcher{f' ({sig})' if sig else ''}"
        if code == 0:
            return True, None
        return False, f"killed by {sig}" if sig else f"exit code {code}"

    async def _stop_orphans(self) -> None:
        """A command of a watcher that died (SIGKILL, crash) may still be running — and trading. It is stopped before
        anything new is handed out; one that cannot be confirmed as ours is left alone and the start refused."""
        names = sorted(p.name for p in self.dir.iterdir())
        for n in (x for x in names if x.startswith("running-") and x.endswith(".pid")):
            await self._stop_orphan(self._path(n))
            with contextlib.suppress(OSError):
                self._path(n).unlink()
        # On Linux every batch's processes are also found by the batch path in their environment — those of a batch
        # whose process record was never written, and any that left the command's process group (setsid). Elsewhere a
        # batch without a record cannot be checked, and it is said so.
        for n in (x for x in names if x.startswith("running-") and x.endswith(".jsonl")):
            if not _is_linux():
                if n[: -len(".jsonl")] + ".pid" in names:
                    continue
                raise RuntimeError(f"a command may have been started for {self._path(n)} just before the previous watcher died, and it cannot be looked for on this system. "
                                   f"Make sure no agent from the previous run is still running, then move that file into {self.dir / 'failed'} and start again")
            for pgid in sorted(_groups_running(os.path.abspath(self._path(n)))):
                self.log({"event": "orphan_command_stopped", "pid": pgid, "note": "a command from the previous run was still running; it was stopped before anything new is handed out"})
                await _stop_group(pgid)

    @staticmethod
    def _kill_batch(pid: int, file: Path) -> None:
        """SIGKILL a batch's command group, and on Linux every process group running the batch (found by its env)."""
        kill = getattr(signal, "SIGKILL", signal.SIGTERM)
        _kill_tree(pid, kill)
        if _is_linux():
            for pgid in _groups_running(os.path.abspath(file)):
                _kill_tree(pgid, kill)

    def _escalate(self, pid: int) -> None:
        """SIGKILL the command's process group in 5 s unless it is over by then."""
        def fire() -> None:
            self._escalations.discard(handle)
            _kill_tree(pid, getattr(signal, "SIGKILL", signal.SIGTERM))
        handle = asyncio.get_running_loop().call_later(5.0, fire)
        self._escalations.add(handle)

    def _recover(self) -> None:
        """After a crash: a batch taken from the queue but never started goes back to the front of it; a batch whose
        command had started is moved to failed/ — it may have been acted on, so it is never run again."""
        names = sorted(p.name for p in self.dir.iterdir())
        batches = [x for x in names if x.startswith("batch-")]
        lines = [line for n in batches for line in _read_lines(self._path(n))]
        # recorded as handed but never started (the process died in between): never run, but never silently dropped
        unstarted = [x for x in lines if _event_id_of(x) in self._handed]
        back = [x for x in lines if _event_id_of(x) not in self._handed]
        if unstarted:
            to = self.dir / "failed" / f"unstarted-{iso(self.now()).replace(':', '-').replace('.', '-')}.jsonl"
            _write_synced(to, "\n".join(_dedupe(unstarted)) + "\n")
            self.log({"event": "batch_unstarted", "fills": len(unstarted), "file": str(to), "note": "recorded as handed to the command, but the watcher stopped before running it: it is not run — check these fills yourself"})
        if back:
            # the merged queue is on disk before any batch file goes: a crash in between only repeats this step
            _write_synced(self._path(QUEUE), "\n".join(_dedupe(back + _read_lines(self._path(QUEUE)))) + "\n")
        for n in batches:
            self._path(n).unlink()
        for n in (x for x in names if x.startswith("running-") and x.endswith(".jsonl")):
            to = self.dir / "failed" / ("interrupted-" + n[len("running-"):])
            os.replace(self._path(n), to)
            self.log({"event": "batch_interrupted", "file": str(to), "note": "the command was running when the watcher stopped; it is not run again — check what it did"})

    async def _stop_orphan(self, pid_file: Path) -> None:
        try:
            rec = json.loads(pid_file.read_bytes())
        except (OSError, ValueError):
            return
        if not isinstance(rec, dict):
            return
        pid = rec.get("pid")
        if not _is_int(pid) or rec.get("host") != socket.gethostname():
            return
        if not _group_alive(pid):
            return
        if not _owns_batch(pid, str(rec.get("batchFile") or "")):
            raise RuntimeError(f"a command from the previous run may still be running (process group {pid}) and could still be trading. "
                               f"Check it, stop it (kill -- -{pid}), then delete {pid_file} and start again")
        self.log({"event": "orphan_command_stopped", "pid": pid, "note": "a command from the previous run was still running; it was stopped before anything new is handed out"})
        await _stop_group(pid)

    def _lock(self) -> None:
        """One watcher per directory: two would both hand every fill to the command. Locks are generations,
        `watch.lock.<n>`, and the highest n is the lock. A new one may be created only when the highest is released or
        held by a dead process on this host, and creating n+1 is exclusive — so of two starters that both saw it free,
        exactly one gets it. Numbers only grow: a released lock stays on disk (marked released) as the highest until a
        newer one exists, and lower ones are never removed — so a starter that paused on an old scan can never create a
        number that was already used. Same files and format as the Node
        package, so a Node and a Python watcher also exclude each other."""
        for _ in range(20):
            gens = self._generations()
            top = gens[-1] if gens else None
            if top is not None:
                owner = self._lock_owner(top)
                if owner is None:  # being written: look again
                    time.sleep(0.025)
                    continue
                if not owner["free"]:
                    pid, host = owner.get("pid"), owner.get("host")
                    raise RuntimeError(f"another pmwallets watch is using {self.dir} (pid {pid if pid is not None else '?'} on {host or '?'}); stop it, or delete {self._path(f'{LOCK}{top}')} if it is gone")
            gen = (top if top is not None else -1) + 1
            file = self._path(f"{LOCK}{gen}")
            try:
                fd = os.open(file, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_BINARY, 0o644)
            except FileExistsError:
                continue
            try:
                _write_all(fd, json.dumps({"pid": os.getpid(), "host": socket.gethostname(), "startedAt": iso(time.time())}, separators=(",", ":")).encode())
                os.fsync(fd)
            finally:
                os.close(fd)
            self._lock_file = file
            # old generations are never removed: no number may be handed out twice, even to a starter whose scan is
            # long stale (each is a few bytes, one per start)
            return
        raise RuntimeError(f"could not lock {self.dir}: other watchers keep starting there")

    def _generations(self) -> list[int]:
        return sorted(int(p.name[len(LOCK):]) for p in self.dir.iterdir()
                      if p.name.startswith(LOCK) and re.fullmatch(r"[0-9]+", p.name[len(LOCK):]))

    def _lock_owner(self, gen: int) -> Optional[dict[str, Any]]:
        """None = unreadable (being written); free = released, or its process is dead on this host."""
        f = self._path(f"{LOCK}{gen}")
        try:
            o = json.loads(f.read_bytes())
        except FileNotFoundError:
            return {"free": True}
        except (OSError, ValueError):
            return _abandoned(f)
        if not isinstance(o, dict):
            return _abandoned(f)
        if o.get("released") is True:
            return {"free": True}
        pid = o.get("pid") if _is_int(o.get("pid")) else None
        host = o.get("host") if isinstance(o.get("host"), str) else None
        if pid is None:
            return _abandoned(f)
        return {"free": host == socket.gethostname() and not _alive(pid), "pid": pid, "host": host}

    def _unlock(self) -> None:
        if self._lock_file is None:
            return
        # released, not removed: the number stays taken, so no starter that scanned earlier can reuse it
        with contextlib.suppress(OSError):
            _write_synced(self._lock_file, json.dumps({"released": True, "at": iso(time.time())}, separators=(",", ":")))
        self._lock_file = None


def _abandoned(f: Path) -> Optional[dict[str, Any]]:
    """A lock file without a readable owner: written within milliseconds of being created, so still unreadable after
    10 s means its writer died — free. Younger: None (being written, look again)."""
    try:
        return {"free": True} if time.time() - f.stat().st_mtime > 10 else None
    except OSError:
        return {"free": True}


class _StoppedWhileStarting(Exception):
    """stop() was called while start() was still running: start() returns quietly."""


def _kill_tree(pid: int, sig: int) -> None:
    """Signal a command and everything it started: its process group on POSIX, `taskkill /T` on Windows."""
    try:
        if sys.platform == "win32":
            subprocess.run(["taskkill", "/pid", str(pid), "/T", "/F"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        else:
            os.killpg(pid, sig)
    except OSError:  # already gone
        pass


def _group_alive(pid: int) -> bool:
    """Is process group `pid` (on Windows: process `pid`) still there?"""
    if sys.platform == "win32":
        return _alive(pid)
    try:
        os.killpg(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _owns_batch(pgid: int, batch_file: str) -> bool:
    """True only when a process of group `pgid` provably runs this batch: Linux, a member whose environment carries
    PMW_BATCH_FILE=<batch_file>. Elsewhere it cannot be proven, and the caller refuses rather than kill a stranger."""
    return _is_linux() and bool(batch_file) and pgid in _groups_running(batch_file)


def _is_linux() -> bool:
    """Processes can be inspected (/proc): only then can a leftover command be found and proven ours."""
    return sys.platform.startswith("linux")


def _groups_running(batch_file: str) -> set[int]:
    """Linux: the process groups with a member whose environment carries PMW_BATCH_FILE=<batch_file>."""
    out: set[int] = set()
    marker = os.fsencode(f"PMW_BATCH_FILE={batch_file}")
    try:
        pids = [d for d in os.listdir("/proc") if d.isdigit()]
    except OSError:
        return out
    for p in pids:
        try:
            if marker not in Path(f"/proc/{p}/environ").read_bytes().split(b"\0"):
                continue
            stat = Path(f"/proc/{p}/stat").read_bytes()
            out.add(int(stat[stat.rindex(b")") + 2:].split(b" ")[2]))  # state, ppid, pgrp, …
        except (OSError, ValueError, IndexError):  # gone, or not ours to read
            continue
    return out


async def _stop_group(pgid: int) -> None:
    """SIGTERM a process group, wait up to 5 s, then SIGKILL."""
    _kill_tree(pgid, signal.SIGTERM)
    for _ in range(50):
        if not _group_alive(pgid):
            break
        await asyncio.sleep(0.1)
    if _group_alive(pgid):
        _kill_tree(pgid, getattr(signal, "SIGKILL", signal.SIGTERM))


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


_INT = re.compile(r"-?[0-9]+")


def _position_of(event_id: str) -> Optional[tuple[int, int]]:
    """(block, logIndex) from an eventId `chain:block:blockHash:txHash:logIndex`."""
    parts = event_id.split(":")
    if len(parts) != 5 or not _INT.fullmatch(parts[1]) or not _INT.fullmatch(parts[4]):
        return None
    return int(parts[1]), int(parts[4])


def _signal_name(code: Optional[int]) -> Optional[str]:
    """The signal that ended a process (a negative return code on POSIX), as SIGTERM / SIGKILL / …"""
    if code is None or code >= 0:
        return None
    try:
        return signal.Signals(-code).name
    except ValueError:
        return f"signal {-code}"


def _event_id_of(line: str) -> str:
    try:
        obj = json.loads(line)
    except ValueError:
        return line
    ev = obj.get("eventId") if isinstance(obj, dict) else None
    return line if ev is None else str(ev)


def _dedupe(lines: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for line in lines:
        try:
            json.loads(line)
        except ValueError:
            continue
        key = _event_id_of(line)
        if key in seen:
            continue
        seen.add(key)
        out.append(line)
    return out


def _read_lines(path: Path) -> list[str]:
    """A file's non-empty lines; a missing file is empty, any other read error is an error."""
    try:
        text = path.read_bytes().decode("utf-8")
    except FileNotFoundError:
        return []
    return [x for x in text.split("\n") if x]


_O_BINARY = getattr(os, "O_BINARY", 0)  # Windows: no "\n" → "\r\n", the files are the same bytes as Node writes


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]


def _append_synced(path: Path, text: str) -> None:
    # a file this creates is not durable until its directory entry is: sync that too, before returning
    created = False
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_APPEND | _O_BINARY, 0o644)
        created = True
    except FileExistsError:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | _O_BINARY, 0o644)
    try:
        _write_all(fd, text.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    if created:
        _sync_dir(path.parent)


def _write_synced(path: Path, text: Union[str, bytes]) -> None:
    """Replace a file atomically and durably: temp file, sync, rename, sync the directory."""
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | _O_BINARY, 0o644)
    try:
        _write_all(fd, text if isinstance(text, bytes) else text.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)
    _sync_dir(path.parent)


def _sync_dir(d: Path) -> None:
    """Make renames and new files in `d` durable; Windows cannot open a directory, and NTFS journals them anyway."""
    if sys.platform == "win32":
        return
    fd = os.open(d, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _alive(pid: int) -> bool:
    if sys.platform == "win32":
        # os.kill(pid, 0) would TerminateProcess on Windows: ask the kernel instead
        import ctypes
        k = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
        h = k.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not h:
            return ctypes.get_last_error() == 5  # access denied: it exists
        try:
            code = ctypes.c_ulong()
            return bool(k.GetExitCodeProcess(h, ctypes.byref(code))) and code.value == 259  # STILL_ACTIVE
        finally:
            k.CloseHandle(h)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _write_stdout(line: str) -> None:
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


def _message(e: Any) -> str:
    return (str(e) or type(e).__name__) if isinstance(e, BaseException) else str(e)
