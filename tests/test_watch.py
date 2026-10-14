import asyncio
import contextlib
import json
import os
import shlex
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from pmwallets import Watcher


def fill(block, log_index=0):
    return {
        "eventId": f"137:{block}:0xh:0xt{block}:{log_index}", "chain": 137, "entityId": "0xe", "wallet": "0xw",
        "ts": "2026-10-14 00:00:00", "block": block, "blockHash": "0xh", "txHash": f"0xt{block}", "logIndex": log_index,
        "exchange": "pm_ctf_v2", "side": "BUY", "role": "taker", "tokenId": "7", "price": "0.42", "shares": "3200000000",
        "usdc": "1344000000", "fee": "0",
    }


_END = object()


class FakeSocket:
    def __init__(self):
        self.q = asyncio.Queue()
        self.close_code = None
        self.close_reason = None

    def send(self, frame):
        self.q.put_nowait(json.dumps(frame))

    async def close(self, code=1000, reason=""):
        if self.close_code is None:
            self.close_code, self.close_reason = code, reason
        self.q.put_nowait(_END)

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self.q.get()
        if item is _END:
            raise StopAsyncIteration
        return item


class FakeClient:
    api_key = "pmw_x_y"
    ws_url = "wss://example/v1/ws"

    async def latency(self):
        return {"head": {"block": 1000}}

    async def fills_since(self, c):  # nothing missed
        return
        yield


def locks(d):
    return sorted(p.name for p in d.iterdir() if p.name.startswith("watch.lock"))


def held_locks(d):
    """lock files held by this process (a released one stays on disk, marked released)"""
    out = []
    for n in locks(d):
        try:
            o = json.loads((d / n).read_text())
        except ValueError:
            continue
        if isinstance(o, dict) and not o.get("released") and o.get("pid") == os.getpid():
            out.append(n)
    return out


class FakeMarkets:
    async def by_token(self, token_id):
        return {"conditionId": "0xc", "question": "Will it rain?", "outcome": "Yes", "slug": "rain", "endDate": None, "closed": False}


def py(code):
    """a shell command running this Python on `code`"""
    return f"{shlex.quote(sys.executable)} -c {shlex.quote(code)}"


# a command that appends its stdin (and the batch file name) to $OUT, one line per batch
RECORD = py("import os,sys,json;d=sys.stdin.read();open(os.environ['OUT'],'a').write(json.dumps({'file':os.environ['PMW_BATCH_FILE'],'size':os.environ['PMW_BATCH_SIZE'],'stdin':d})+'\\n')")


class Harness:
    def __init__(self, dir, exec=None, exec_timeout_sec=600.0):
        self.dir = dir
        self.sockets, self.lines, self.events = [], [], []
        self.seq = 0

        @contextlib.asynccontextmanager
        async def connector(url, headers):
            s = FakeSocket()
            self.sockets.append(s)
            yield s

        self.w = Watcher(FakeClient(), dir, exec=exec, batch_sec=0, exec_timeout_sec=exec_timeout_sec, markets=FakeMarkets(),
                         out=self.lines.append, log=self.events.append,
                         stream_options={"connector": connector, "min_backoff": 0.001, "max_backoff": 0.002})

    def push(self, f):
        self.seq += 1
        self.sockets[-1].send({"type": "fill", "session": "s1", "seq": self.seq, "data": f})

    def hello(self):
        self.sockets[-1].send({"type": "hello", "session": "s1", "seq": 0})

    def has(self, name, n=1):
        return sum(1 for e in self.events if e["event"] == name) >= n

    def find(self, name):
        return next(e for e in self.events if e["event"] == name)

    async def ready(self):
        await self.w.start()
        await until(lambda: self.has("connected"))
        self.hello()
        await until(lambda: self.has("hello"))


async def until(cond, timeout=10.0):
    end = time.monotonic() + timeout
    while not cond():
        if time.monotonic() > end:
            raise AssertionError("timed out")
        await asyncio.sleep(0.01)


@pytest.fixture
async def harness():
    made = []

    def make(dir, **kw):
        h = Harness(dir, **kw)
        made.append(h)
        return h

    yield make
    for h in made:
        await h.w.stop()


async def test_without_a_command_prints_one_line_per_fill_with_decimals_and_market(tmp_path, harness):
    h = harness(tmp_path)
    await h.ready()
    h.push(fill(1001))
    await until(lambda: len(h.lines) == 1)
    f = json.loads(h.lines[0])
    assert f["eventId"] == fill(1001)["eventId"]
    assert f["usdcAmount"] == "1344.000000"
    assert f["sharesAmount"] == "3200.000000"
    assert f["feeAmount"] == "0.000000"
    assert f["market"]["question"] == "Will it rain?"
    assert f["market"]["outcome"] == "Yes"
    assert f["source"] == "ws"
    assert f["receivedAt"].endswith("Z") and len(f["receivedAt"]) == 24
    assert json.loads((tmp_path / "stream.json").read_text())["seq"] == 1


async def test_an_output_that_returns_a_future_is_awaited_before_the_fill_counts(tmp_path, harness):
    h = harness(tmp_path)
    pending: list[asyncio.Future] = []

    def out(line):
        f = asyncio.get_running_loop().create_future()
        pending.append(f)
        return f

    h.w.out = out
    await h.ready()
    h.push(fill(1001))
    await until(lambda: len(pending) == 1)
    await asyncio.sleep(0.1)
    # not done yet: the stream must not have moved past the fill
    assert json.loads((tmp_path / "stream.json").read_text())["seq"] == 0
    pending[0].set_result(None)
    await until(lambda: json.loads((tmp_path / "stream.json").read_text())["seq"] == 1)


async def test_market_falls_back_to_the_frame_title(tmp_path, harness):
    h = harness(tmp_path)
    h.w.markets = None
    await h.ready()
    h.push({**fill(1001), "marketTitle": "Will it snow?", "conditionId": "0xcc"})
    await until(lambda: len(h.lines) == 1)
    assert json.loads(h.lines[0])["market"] == {"conditionId": "0xcc", "question": "Will it snow?", "outcome": None, "slug": None, "endDate": None, "closed": None}


async def test_hands_fills_to_the_command_on_stdin_and_batches_the_ones_arriving_while_it_runs(tmp_path, harness, monkeypatch):
    out = tmp_path / "calls.jsonl"
    monkeypatch.setenv("OUT", str(out))
    d = tmp_path / "w"
    h = harness(d, exec=f"{RECORD} && {py('import time; time.sleep(0.3)')}")
    await h.ready()
    h.push(fill(1001))
    await until(lambda: h.has("batch_start"))
    h.push(fill(1002))
    h.push(fill(1003))
    await until(lambda: h.has("batch_done", 2))
    calls = [json.loads(x) for x in out.read_text().strip().split("\n")]
    assert [c["size"] for c in calls] == ["1", "2"]
    assert [e["queued"] for e in h.events if e["event"] == "batch_start"] == [0, 0]
    second = [json.loads(x) for x in calls[1]["stdin"].strip().split("\n")]
    assert [f["block"] for f in second] == [1002, 1003]
    assert "/running-" in calls[0]["file"] and calls[0]["file"].endswith(".jsonl")
    # a finished batch leaves nothing behind
    assert [p.name for p in d.iterdir() if p.name.startswith(("running-", "batch-"))] == []
    assert (d / "queue.jsonl").read_text() == ""


async def test_failing_or_hanging_command_moves_its_batch_to_failed(tmp_path, harness):
    h = harness(tmp_path / "a", exec=py("import sys; sys.exit(3)"))
    await h.ready()
    h.push(fill(1001))
    await until(lambda: h.has("batch_failed"))
    failed = h.find("batch_failed")
    assert failed["reason"] == "exit code 3"
    assert failed["eventIds"] == [fill(1001)["eventId"]]
    assert len(list((tmp_path / "a" / "failed").iterdir())) == 1

    t = harness(tmp_path / "b", exec=py("import time; time.sleep(10)"), exec_timeout_sec=0.2)
    await t.ready()
    t.push(fill(1001))
    await until(lambda: t.has("batch_failed"))
    assert "timed out" in t.find("batch_failed")["reason"]
    # and the next batch still runs
    t.w.exec = py("pass")
    t.push(fill(1002))
    await until(lambda: t.has("batch_done"))


async def test_a_command_ignoring_stdin_is_not_a_failure(tmp_path, harness):
    h = harness(tmp_path, exec=py("pass"))
    await h.ready()
    h.push({**fill(1001), "pad": "x" * 200_000})  # bigger than a pipe buffer
    await until(lambda: h.has("batch_done") or h.has("batch_failed"))
    assert h.has("batch_done")


async def test_after_a_crash_a_started_batch_is_never_rerun_and_an_unstarted_one_is_queued_again(tmp_path, harness, monkeypatch):
    d = tmp_path / "w"
    (d / "failed").mkdir(parents=True)
    started = json.dumps({**fill(900), "source": "ws"})
    not_started = json.dumps({**fill(901), "source": "ws"})
    queued = json.dumps({**fill(902), "source": "ws"})
    (d / "running-a.jsonl").write_text(started + "\n")
    (d / "batch-b.jsonl").write_text(not_started + "\n")
    # the queue was not yet rewritten when it died: the same fill is in both, and must go out once
    (d / "queue.jsonl").write_text(f"{not_started}\n{queued}\n")
    out = tmp_path / "calls.jsonl"
    monkeypatch.setenv("OUT", str(out))
    h = harness(d, exec=RECORD)
    await h.w.start()
    await until(lambda: h.has("batch_done"))
    assert h.has("batch_interrupted")
    assert [p.name for p in (d / "failed").iterdir()] == ["interrupted-a.jsonl"]
    calls = [json.loads(x) for x in out.read_text().strip().split("\n")]
    assert len(calls) == 1
    assert [json.loads(x)["block"] for x in calls[0]["stdin"].strip().split("\n")] == [901, 902]


async def test_a_dispatch_that_throws_stops_with_code_1(tmp_path, harness):
    h = harness(tmp_path, exec=py("pass"))
    await h.ready()

    async def broken():
        raise OSError("disk full")
    h.w._dispatch = broken
    h.push(fill(1001))
    assert await asyncio.wait_for(h.w.wait(), 5) == 1
    assert h.find("fatal")["error"] == "batch dispatch failed: disk full"


async def test_refuses_a_second_watcher_on_the_same_directory(tmp_path, harness):
    h = harness(tmp_path)
    await h.w.start()
    assert locks(tmp_path) == ["watch.lock.0"]
    lock = json.loads((tmp_path / "watch.lock.0").read_text())
    assert list(lock) == ["pid", "host", "startedAt"]
    w2 = Watcher(FakeClient(), tmp_path)
    with pytest.raises(RuntimeError, match="another pmwallets watch"):
        await w2.start()
    await h.w.stop()
    assert held_locks(tmp_path) == []


async def test_a_lock_left_by_a_dead_process_on_this_host_is_taken_over(tmp_path, harness):
    p = subprocess.Popen([sys.executable, "-c", "pass"])
    p.wait()
    (tmp_path / "watch.lock.0").write_text(json.dumps({"pid": p.pid, "host": socket.gethostname(), "startedAt": "x"}))
    h = harness(tmp_path)
    await h.w.start()
    import os
    assert held_locks(tmp_path) == ["watch.lock.1"]


async def test_a_lock_from_another_host_is_never_taken_over(tmp_path):
    (tmp_path / "watch.lock.0").write_text(json.dumps({"pid": 1, "host": "elsewhere", "startedAt": "x"}))
    with pytest.raises(RuntimeError, match="pid 1 on elsewhere"):
        await Watcher(FakeClient(), tmp_path).start()


async def test_fatal_stream_error_stops_with_code_2(tmp_path, harness):
    h = harness(tmp_path)
    await h.w.start()
    await until(lambda: h.has("connected"))
    h.sockets[-1].send({"type": "blocked"})
    await h.sockets[-1].close(4411, "seat taken")
    assert await asyncio.wait_for(h.w.wait(), 5) == 2
    assert "takeover" in h.find("fatal")["error"]
    assert held_locks(tmp_path) == []


def _gone(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


@pytest.mark.skipif(sys.platform == "win32", reason="process groups are POSIX")
async def test_a_timeout_or_a_stop_ends_the_whole_command_tree_and_the_lock_is_held_until_the_batch_is_over(tmp_path, harness):
    pid_file = tmp_path / "grandchild.pid"
    # a command that starts a long-lived grandchild (as an agent CLI does) and waits for it
    tree = py(
        "import subprocess,sys,time;"
        "c=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);"
        f"open({str(pid_file)!r},'w').write(str(c.pid));time.sleep(60)")
    t = harness(tmp_path / "t", exec=tree, exec_timeout_sec=0.3)
    await t.ready()
    t.push(fill(1001))
    await until(lambda: t.has("batch_failed"), 8)
    assert "timed out" in t.find("batch_failed")["reason"]
    gc = int(pid_file.read_text())
    await until(lambda: _gone(gc), 8)

    s = harness(tmp_path / "s", exec=tree, exec_timeout_sec=60)
    await s.ready()
    s.push(fill(2001))
    await until(lambda: s.has("batch_start"))
    await until(lambda: pid_file.exists() and pid_file.read_text() not in ("", str(gc)))
    gc2 = int(pid_file.read_text())
    await s.w.stop()
    # by the time stop() returns: the batch is in failed/, the command tree is gone, and the lock is released
    assert "stopped with the watcher" in s.find("batch_failed")["reason"]
    # the command is over: no SIGKILL may still be pending for its (soon reusable) process id
    assert s.w._escalations == set()
    assert len(list((tmp_path / "s" / "failed").iterdir())) == 1
    assert not any(p.name.startswith("running-") for p in (tmp_path / "s").iterdir())
    assert held_locks(tmp_path / "s") == []
    await until(lambda: _gone(gc2), 8)


async def test_a_grandchild_holding_stdin_does_not_keep_the_batch_open(tmp_path, harness):
    # the command exits at once, leaving a child that inherited its stdin and never reads it
    cmd = py("import subprocess,sys; subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)'])")
    h = harness(tmp_path, exec=cmd, exec_timeout_sec=20)
    await h.ready()
    h.push({**fill(1001), "pad": "x" * 300_000})  # more than a pipe buffer: the stdin pipe cannot be flushed
    started = time.monotonic()
    await until(lambda: h.has("batch_done") or h.has("batch_failed"), 15)
    assert h.has("batch_done") and time.monotonic() - started < 10


async def test_a_fill_already_handed_to_the_command_is_never_handed_again(tmp_path, harness, monkeypatch):
    d = tmp_path / "w"
    d.mkdir()
    # the last run handed 1001 to the command and died before the stream cursor was saved; it had also taken 1003 off
    # the queue and recorded it as handed, but not started the command yet
    (d / "batch-x.jsonl").write_text(json.dumps({**fill(1003), "source": "ws"}) + "\n")
    (d / "handed.txt").write_text(f"{fill(1001)['eventId']}\n{fill(1003)['eventId']}\n")
    out = tmp_path / "calls.jsonl"
    monkeypatch.setenv("OUT", str(out))
    h = harness(d, exec=RECORD)
    await h.ready()
    h.push(fill(1001))
    h.push(fill(1002))
    await until(lambda: h.has("batch_done"))
    await asyncio.sleep(0.05)
    assert any(e["event"] == "skipped_already_handed" and e["eventId"] == fill(1001)["eventId"] for e in h.events)
    calls = [json.loads(x) for x in out.read_text().strip().split("\n")]
    assert [json.loads(x)["block"] for c in calls for x in c["stdin"].strip().split("\n")] == [1002]
    assert fill(1002)["eventId"] in (d / "handed.txt").read_text().split("\n")
    # 1003 was never run, and is kept for the user rather than dropped
    unstarted = [p for p in (d / "failed").iterdir() if p.name.startswith("unstarted-")]
    assert len(unstarted) == 1
    assert fill(1003)["eventId"] in unstarted[0].read_text()
    assert h.has("batch_unstarted")
    assert not any(p.name.startswith("batch-") for p in d.iterdir())


async def test_forgets_handed_fills_only_at_or_before_the_saved_stream_cursor(tmp_path, harness, monkeypatch):
    d = tmp_path / "w"
    d.mkdir()
    old = [fill(100 + i)["eventId"] for i in range(40_001)]
    fresh = [fill(50_000, 1)["eventId"], fill(50_001)["eventId"]]
    (d / "handed.txt").write_text("\n".join(old + fresh) + "\n")
    # the stream has saved a position past the old fills but not the fresh ones (a replay still going on)
    (d / "stream.json").write_text(json.dumps({"session": "s0", "seq": 0, "block": 50_000, "logIndex": 0}))
    # queued before the start: dispatched while the stream has not saved anything newer (in Python the stream
    # saves its cursor before the batch timer fires, which would make the expected set depend on timing)
    (d / "queue.jsonl").write_text(json.dumps({**fill(60_000), "source": "ws"}) + "\n")
    out = tmp_path / "calls.jsonl"
    monkeypatch.setenv("OUT", str(out))
    h = harness(d, exec=RECORD)
    await h.w.start()
    await until(lambda: h.has("batch_done"), 15)
    kept = (d / "handed.txt").read_text().strip().split("\n")
    assert sorted(kept) == sorted(fresh + [fill(60_000)["eventId"]])
    assert h.w._handed_limit == 40_000


def _detached(code, env):
    """start `code` in its own process group, NOT as our child (so init reaps it when it dies); returns its pid"""
    r = subprocess.run([sys.executable, "-c",
                        "import subprocess,sys;"
                        f"p=subprocess.Popen([sys.executable,'-c',{code!r}],start_new_session=True,"
                        "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);print(p.pid)"],
                       capture_output=True, text=True, env=env, check=True)
    return int(r.stdout)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="ownership is proven through /proc")
async def test_stops_a_command_the_previous_watcher_left_running_and_refuses_one_it_cannot_prove_is_ours(tmp_path, harness):
    d = tmp_path / "w"
    (d / "failed").mkdir(parents=True)
    batch_file = d / "running-a.jsonl"
    batch_file.write_text(json.dumps({**fill(900), "source": "ws"}) + "\n")
    # the old watcher's command, still running in its own process group with the batch in its environment
    env = {k: v for k, v in os.environ.items() if k != "PMW_BATCH_FILE"}
    orphan = _detached("import time; time.sleep(60)", {**env, "PMW_BATCH_FILE": str(batch_file)})
    (d / "running-a.pid").write_text(json.dumps({"pid": orphan, "host": socket.gethostname(), "batchFile": str(batch_file)}))
    h = harness(d, exec=py("pass"))
    await h.w.start()
    assert h.has("orphan_command_stopped")
    await until(lambda: _gone(orphan))
    assert [p.name for p in (d / "failed").iterdir()] == ["interrupted-a.jsonl"]
    assert [p.name for p in d.iterdir() if p.name.endswith(".pid")] == []
    await h.w.stop()

    # a live process group the record cannot be matched to: never killed, the start is refused and the lock released
    stranger = _detached("import time; time.sleep(60)", env)
    try:
        (d / "running-b.jsonl").write_text(json.dumps({**fill(901), "source": "ws"}) + "\n")
        (d / "running-b.pid").write_text(json.dumps({"pid": stranger, "host": socket.gethostname(), "batchFile": str(d / "running-b.jsonl")}))
        with pytest.raises(RuntimeError, match="may still be running"):
            await harness(d, exec=py("pass")).w.start()
        assert held_locks(d) == []
        assert not _gone(stranger)
    finally:
        os.killpg(stranger, 9)


async def test_a_running_batch_records_who_runs_it_until_it_is_over(tmp_path, harness):
    ready = tmp_path / "ready"
    h = harness(tmp_path / "w", exec=py(f"import time; open({str(ready)!r},'w').close(); time.sleep(0.5)"))
    await h.ready()
    h.push(fill(1001))
    await until(ready.exists)
    [pid_file] = [p for p in (tmp_path / "w").iterdir() if p.name.endswith(".pid")]
    rec = json.loads(pid_file.read_text())
    assert rec["host"] == socket.gethostname() and rec["batchFile"].endswith(pid_file.name[:-4] + ".jsonl") and rec["pid"] > 0
    await until(lambda: h.has("batch_done"))
    assert not pid_file.exists()


async def test_takes_over_a_dead_lock_with_the_next_generation_and_two_starters_cannot_both_get_it(tmp_path, harness):
    dead = json.dumps({"pid": 2 ** 22 + 12345, "host": socket.gethostname()})
    (tmp_path / "watch.lock.0").write_text(dead)
    (tmp_path / "watch.lock.3").write_text(dead)
    h = harness(tmp_path)
    await h.w.start()
    # ours is the next one
    assert held_locks(tmp_path) == ["watch.lock.4"]
    assert json.loads((tmp_path / "watch.lock.4").read_text())["pid"] == os.getpid()
    # a second starter sees a live holder and refuses, without touching its file
    with pytest.raises(RuntimeError, match="another pmwallets watch"):
        await Watcher(FakeClient(), tmp_path).start()
    assert (tmp_path / "watch.lock.4").exists()
    await h.w.stop()
    assert held_locks(tmp_path) == []
    # released, not removed: every number stays taken, and the next watcher goes higher, never back to an old one
    assert locks(tmp_path) == ["watch.lock.0", "watch.lock.3", "watch.lock.4"]
    assert json.loads((tmp_path / "watch.lock.4").read_text())["released"] is True
    again = harness(tmp_path)
    await again.w.start()
    assert locks(tmp_path) == ["watch.lock.0", "watch.lock.3", "watch.lock.4", "watch.lock.5"]
    await again.w.stop()
    # a starter whose scan is stale tries a number already used: it exists, so it cannot be taken twice
    with pytest.raises(FileExistsError):
        os.open(tmp_path / "watch.lock.4", os.O_WRONLY | os.O_CREAT | os.O_EXCL)

    # a starter that saw the dead holder but lost the race for the next generation finds a live one and refuses
    (tmp_path / "watch.lock.17").write_text(dead)
    (tmp_path / "watch.lock.18").write_text(json.dumps({"pid": os.getpid(), "host": socket.gethostname()}))
    with pytest.raises(RuntimeError, match="another pmwallets watch"):
        await Watcher(FakeClient(), tmp_path).start()


async def test_losing_the_race_for_the_next_generation_refuses(tmp_path, monkeypatch):
    import pmwallets.watch as watch
    (tmp_path / "watch.lock.0").write_text(json.dumps({"pid": 2 ** 22 + 12345, "host": socket.gethostname()}))
    real_alive = watch._alive

    def alive_then_race(pid):
        # another starter saw the same dead holder and created generation 1 first
        if not (tmp_path / "watch.lock.1").exists():
            (tmp_path / "watch.lock.1").write_text(json.dumps({"pid": os.getpid(), "host": socket.gethostname()}))
        return real_alive(pid)
    monkeypatch.setattr(watch, "_alive", alive_then_race)
    with pytest.raises(RuntimeError, match="another pmwallets watch"):
        await Watcher(FakeClient(), tmp_path).start()
    assert locks(tmp_path) == ["watch.lock.0", "watch.lock.1"]  # the winner's file untouched


async def test_a_lock_file_left_empty_by_a_starter_that_died_is_taken_over_once_it_is_clearly_abandoned(tmp_path, harness):
    (tmp_path / "watch.lock.2").write_text("")
    with pytest.raises(RuntimeError, match="could not lock"):  # may still be being written
        await harness(tmp_path).w.start()
    old = time.time() - 60
    os.utime(tmp_path / "watch.lock.2", (old, old))
    h = harness(tmp_path)
    await h.w.start()
    assert held_locks(tmp_path) == ["watch.lock.3"]


async def test_a_queue_that_cannot_be_read_is_an_error_not_an_empty_queue(tmp_path, harness):
    (tmp_path / "queue.jsonl").mkdir()  # reading it fails with something other than "not found"
    (tmp_path / "batch-x.jsonl").write_text(json.dumps(fill(1)) + "\n")
    with pytest.raises(OSError):
        await harness(tmp_path, exec=py("pass")).w.start()
    assert (tmp_path / "batch-x.jsonl").exists()  # the batch is not dropped


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")
async def test_stop_kills_a_command_that_ignores_sigterm_after_5_s(tmp_path, harness):
    ready = tmp_path / "ready"
    # `exec`: the command itself (not a shell around it) ignores SIGTERM
    cmd = "exec " + py(f"import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); open({str(ready)!r},'w').close(); time.sleep(60)")
    h = harness(tmp_path / "w", exec=cmd, exec_timeout_sec=60)
    await h.ready()
    h.push(fill(1001))
    await until(ready.exists)
    started = time.monotonic()
    await h.w.stop()
    assert 4 < time.monotonic() - started < 9
    assert h.find("batch_failed")["reason"] == "stopped with the watcher (SIGKILL)"
    assert h.w._escalations == set()
    assert held_locks(tmp_path / "w") == []


async def test_a_start_that_fails_unreadable_stream_position_releases_the_directory(tmp_path, harness):
    (tmp_path / "stream.json").write_text("{not json")
    h = harness(tmp_path)
    with pytest.raises(ValueError):
        await h.w.start()
    assert held_locks(tmp_path) == []
    # and the directory can be used again once the position is fixed
    (tmp_path / "stream.json").unlink()
    await h.w.start()
    assert len(held_locks(tmp_path)) == 1


@pytest.mark.skipif(sys.platform == "win32", reason="process groups are POSIX")
async def test_checks_for_a_leftover_command_in_every_mode_not_only_with_exec(tmp_path, harness):
    stranger = _detached("import time; time.sleep(60)", {k: v for k, v in os.environ.items() if k != "PMW_BATCH_FILE"})
    try:
        (tmp_path / "running-c.pid").write_text(json.dumps({"pid": stranger, "host": socket.gethostname(), "batchFile": str(tmp_path / "running-c.jsonl")}))
        h = harness(tmp_path)  # stdout mode
        with pytest.raises(RuntimeError, match="may still be running"):
            await h.w.start()
        assert held_locks(tmp_path) == []
    finally:
        os.killpg(stranger, 9)


async def test_a_command_whose_process_id_cannot_be_recorded_is_stopped_and_its_batch_failed(tmp_path, harness, monkeypatch):
    from pmwallets.watch import iso
    at = 1_791_936_000.0  # 2026-10-14T00:00:00Z
    bid = f"{iso(at).replace(':', '-').replace('.', '-')}-{os.getpid()}"
    (tmp_path / f"running-{bid}.pid").mkdir()  # a directory where the record must go
    (tmp_path / "queue.jsonl").write_text(json.dumps({**fill(700), "source": "ws"}) + "\n")
    out = tmp_path / "calls.jsonl"
    monkeypatch.setenv("OUT", str(out))
    h = harness(tmp_path, exec=f"{py('import time; time.sleep(1)')} && {RECORD}")
    h.w.now = lambda: at
    await h.w.start()
    await until(lambda: h.has("batch_failed"))
    assert "could not record the command's process id" in h.find("batch_failed")["reason"]
    await asyncio.sleep(1.5)
    assert not out.exists()  # the command never got to act


async def test_a_stop_during_start_returns_quietly_and_releases_the_directory(tmp_path, harness):
    h = harness(tmp_path)
    real = h.w._stop_orphans

    async def slow():
        await asyncio.sleep(0.2)
        await real()
    h.w._stop_orphans = slow
    starting = asyncio.create_task(h.w.start())
    await asyncio.sleep(0.05)
    await h.w.stop()  # waits for the start, which sees the stop and cleans up
    await starting  # no exception
    assert await asyncio.wait_for(h.w.wait(), 1) == 0
    assert h.w.stream is None and held_locks(tmp_path) == []


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")
async def test_a_stop_while_the_command_is_being_started_runs_the_full_stop_path(tmp_path, harness):
    ready = tmp_path / "ready"
    # ignores SIGTERM from its first instruction on (`trap` before `exec`: the ignore survives the exec)
    cmd = "trap '' TERM; exec " + py(f"import time; open({str(ready)!r},'w').close(); time.sleep(60)")
    d = tmp_path / "w"
    h = harness(d, exec=cmd, exec_timeout_sec=60)
    await h.ready()
    loop = asyncio.get_running_loop()
    real = loop.subprocess_shell
    spawned, release, pids = asyncio.Event(), asyncio.Event(), []

    async def slow_spawn(*a, **k):
        r = await real(*a, **k)
        pids.append(r[0].get_pid())
        spawned.set()
        await release.wait()  # the process exists, but the watcher has not seen its pid yet
        return r
    loop.subprocess_shell = slow_spawn
    try:
        h.push(fill(1001))
        await spawned.wait()
        await until(ready.exists)
        t0 = time.monotonic()
        stopping = asyncio.create_task(h.w.stop())
        await asyncio.sleep(0.1)
        release.set()
        await asyncio.wait_for(stopping, 9)
    finally:
        del loop.subprocess_shell
    took = time.monotonic() - t0
    assert 4 < took < 8  # SIGTERM ignored, SIGKILL after 5 s — not the 60 s timeout
    assert h.w._escalations == set()
    assert "stopped with the watcher" in h.find("batch_failed")["reason"]
    assert len(list((d / "failed").iterdir())) == 1
    assert held_locks(d) == []
    with pytest.raises(ProcessLookupError):
        os.killpg(pids[0], 0)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="processes are found through /proc")
async def test_a_stop_while_start_is_still_stopping_a_leftover_command_keeps_the_lock_until_start_has_finished(tmp_path, harness):
    batch_file = tmp_path / "running-s.jsonl"
    batch_file.write_text(json.dumps({**fill(810), "source": "ws"}) + "\n")
    env = {k: v for k, v in os.environ.items() if k != "PMW_BATCH_FILE"}
    # ignores SIGTERM, so stopping it takes the full 5 s grace
    orphan = _detached("import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)", {**env, "PMW_BATCH_FILE": str(batch_file)})
    try:
        await asyncio.sleep(0.5)
        h = harness(tmp_path, exec=py("pass"))
        started = asyncio.create_task(h.w.start())
        await until(lambda: h.has("orphan_command_stopped"))
        stopping = asyncio.create_task(h.w.stop())
        await asyncio.sleep(0.2)
        # start is still busy with the leftover: the directory is still ours, and recovery has not run
        assert len(held_locks(tmp_path)) == 1
        assert batch_file.exists()
        await started
        await stopping
        assert held_locks(tmp_path) == []
        assert batch_file.exists()  # a stopped start does not touch the batch files
        await until(lambda: _gone(orphan), 8)
    finally:
        with contextlib.suppress(OSError):
            os.killpg(orphan, 9)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="processes are found through /proc")
async def test_finds_a_leftover_command_by_its_batch_path_even_when_its_process_record_was_never_written(tmp_path, harness):
    batch_file = tmp_path / "running-z.jsonl"
    batch_file.write_text(json.dumps({**fill(800), "source": "ws"}) + "\n")
    env = {k: v for k, v in os.environ.items() if k != "PMW_BATCH_FILE"}
    orphan = _detached("import time; time.sleep(60)", {**env, "PMW_BATCH_FILE": str(batch_file)})
    try:
        h = harness(tmp_path)  # stdout mode
        await h.w.start()
        assert any(e["event"] == "orphan_command_stopped" and e["pid"] == orphan for e in h.events)
        await until(lambda: _gone(orphan))
    finally:
        with contextlib.suppress(OSError):
            os.killpg(orphan, 9)


async def test_the_command_gets_an_absolute_batch_path(tmp_path, harness, monkeypatch):
    out = tmp_path / "calls.jsonl"
    monkeypatch.setenv("OUT", str(out))
    monkeypatch.chdir(tmp_path)
    h = harness(Path("rel"), exec=RECORD)
    await h.ready()
    h.push(fill(1001))
    await until(lambda: h.has("batch_done"))
    assert json.loads(out.read_text())["file"] == str(tmp_path / "rel" / json.loads(out.read_text())["file"].rsplit("/", 1)[1])


async def test_without_exec_refuses_a_directory_that_still_holds_fills_queued_for_a_command(tmp_path, harness):
    (tmp_path / "queue.jsonl").write_text(json.dumps({**fill(900), "source": "ws"}) + "\n")
    with pytest.raises(RuntimeError, match="queued for a command"):
        await harness(tmp_path).w.start()
    assert held_locks(tmp_path) == []
    (tmp_path / "queue.jsonl").write_text("")
    (tmp_path / "batch-x.jsonl").write_text(json.dumps(fill(901)) + "\n")
    with pytest.raises(RuntimeError, match="queued for a command"):
        await harness(tmp_path).w.start()
    (tmp_path / "batch-x.jsonl").unlink()
    h = harness(tmp_path)  # an empty queue is fine
    await h.w.start()
    assert len(held_locks(tmp_path)) == 1


async def test_more_than_max_batch_queued_runs_in_several_batches(tmp_path, harness, monkeypatch):
    out = tmp_path / "calls.jsonl"
    monkeypatch.setenv("OUT", str(out))
    d = tmp_path / "w"
    d.mkdir()
    (d / "queue.jsonl").write_text("".join(json.dumps({**fill(b), "source": "ws"}) + "\n" for b in range(1, 6)))
    h = harness(d, exec=RECORD)
    h.w.max_batch = 2
    await h.w.start()
    await until(lambda: h.has("batch_done", 3))
    await asyncio.sleep(0.1)
    assert [c["size"] for c in map(json.loads, out.read_text().strip().split("\n"))] == ["2", "2", "1"]
    assert [e["queued"] for e in h.events if e["event"] == "batch_start"] == [3, 1, 0]
    assert h.w._pending is False


async def test_where_processes_cannot_be_inspected_a_batch_that_may_have_a_leftover_command_blocks_the_start(tmp_path, harness, monkeypatch):
    import pmwallets.watch as watch
    (tmp_path / "running-q.jsonl").write_text(json.dumps({**fill(801), "source": "ws"}) + "\n")
    monkeypatch.setattr(watch, "_is_linux", lambda: False)
    with pytest.raises(RuntimeError, match="cannot be looked for on this system"):
        await harness(tmp_path, exec=py("pass")).w.start()
    assert held_locks(tmp_path) == []
    assert (tmp_path / "running-q.jsonl").exists()  # left for the person to check


async def test_lock_generations_are_never_removed(tmp_path, harness):
    dead = json.dumps({"pid": 2 ** 22 + 12345, "host": socket.gethostname()})
    old = time.time() - 7200
    for g in (0, 1):
        (tmp_path / f"watch.lock.{g}").write_text(dead)
        os.utime(tmp_path / f"watch.lock.{g}", (old, old))
    (tmp_path / "watch.lock.150").write_text(dead)
    h = harness(tmp_path)
    await h.w.start()
    assert locks(tmp_path) == ["watch.lock.0", "watch.lock.1", "watch.lock.150", "watch.lock.151"]


async def test_handed_ids_are_kept_when_the_stream_position_cannot_be_synced(tmp_path, harness, monkeypatch):
    import pmwallets.watch as watch
    (tmp_path / "stream.json").write_text(json.dumps({"session": "s0", "seq": 0, "block": 50_000, "logIndex": 0}))
    h = harness(tmp_path)
    assert h.w._saved_cursor() == (50_000, 0)
    synced = []
    real = os.fsync
    monkeypatch.setattr(watch.os, "fsync", lambda fd: synced.append(fd) or real(fd))
    h.w._saved_cursor()
    assert synced  # the position is synced before it is trusted

    def broken(fd):
        raise OSError("EIO")
    monkeypatch.setattr(watch.os, "fsync", broken)
    assert h.w._saved_cursor() is None  # no cursor: nothing is forgotten


def test_appending_to_a_new_file_also_syncs_its_directory(tmp_path, monkeypatch):
    import pmwallets.watch as watch
    synced = []
    monkeypatch.setattr(watch, "_sync_dir", synced.append)
    f = tmp_path / "queue.jsonl"
    watch._append_synced(f, "a\n")
    assert synced == [tmp_path]  # created: its directory entry is made durable too
    watch._append_synced(f, "b\n")
    assert synced == [tmp_path]  # existing: the file sync is enough
    assert f.read_text() == "a\nb\n"


async def test_a_half_written_last_line_is_cut_off_so_the_replayed_fill_is_not_glued_onto_it(tmp_path, harness, monkeypatch):
    d = tmp_path / "w"
    d.mkdir()
    whole = json.dumps({**fill(950), "source": "ws"})
    (d / "queue.jsonl").write_text(whole + "\n" + json.dumps({**fill(951), "source": "ws"})[:40])
    (d / "handed.txt").write_text("137:1:h:t:0\n137:2:h")  # the record can be cut the same way
    out = tmp_path / "calls.jsonl"
    monkeypatch.setenv("OUT", str(out))
    h = harness(d, exec=RECORD)
    h.w.batch_sec = 0.3
    await h.w.start()
    removed = {Path(e["file"]).name: e["bytes"] for e in h.events if e["event"] == "partial_line_removed"}
    assert removed == {"queue.jsonl": 40, "handed.txt": len("137:2:h")}
    assert (d / "handed.txt").read_text() == "137:1:h:t:0\n"
    await until(lambda: h.has("connected"))
    h.hello()
    await until(lambda: h.has("hello"))
    h.push(fill(951))  # the stream sends the unconfirmed fill again
    await until(lambda: h.has("batch_done"))
    await until(lambda: out.exists() and sum(len(json.loads(c)["stdin"].strip().split("\n")) for c in out.read_text().strip().split("\n")) >= 2)
    sent = [json.loads(x)["block"] for c in out.read_text().strip().split("\n") for x in json.loads(c)["stdin"].strip().split("\n")]
    assert sent == [950, 951]


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="processes are found through /proc")
async def test_also_stops_a_leftover_that_left_the_group_even_when_the_batch_has_a_process_record(tmp_path, harness):
    batch_file = tmp_path / "running-g.jsonl"
    batch_file.write_text(json.dumps({**fill(820), "source": "ws"}) + "\n")
    # the recorded group is gone; a process that escaped it (its own session) still runs the batch
    (tmp_path / "running-g.pid").write_text(json.dumps({"pid": 2 ** 22 + 4321, "host": socket.gethostname(), "batchFile": str(batch_file)}))
    env = {k: v for k, v in os.environ.items() if k != "PMW_BATCH_FILE"}
    escaped = _detached("import time; time.sleep(60)", {**env, "PMW_BATCH_FILE": str(batch_file)})
    try:
        await asyncio.sleep(0.2)
        h = harness(tmp_path, exec=py("pass"))
        await h.w.start()
        await until(lambda: _gone(escaped))
    finally:
        with contextlib.suppress(OSError):
            os.killpg(escaped, 9)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="processes are found through /proc")
async def test_when_a_batch_ends_a_process_that_left_the_group_is_stopped_too(tmp_path, harness):
    pid_file = tmp_path / "escaped.pid"
    # the command starts a child in its own session and exits at once
    cmd = py("import subprocess,sys;"
             "c=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'],start_new_session=True,"
             "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);"
             f"open({str(pid_file)!r},'w').write(str(c.pid))")
    h = harness(tmp_path / "w", exec=cmd)
    await h.ready()
    h.push(fill(1001))
    await until(lambda: h.has("batch_done"))
    escaped = int(pid_file.read_text())
    try:
        await until(lambda: _gone(escaped))
    finally:
        with contextlib.suppress(OSError):
            os.killpg(escaped, 9)
