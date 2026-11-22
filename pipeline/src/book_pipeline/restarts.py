"""Recorder restarts that left no `disconnect` line.

A killed recorder writes no `disconnect`, and a stopping one does not always manage to. Read literally, its
connections stay up for the rest of the hour: coverage reports no gap and replay runs straight across the outage.

The evidence of a new process is in the raw: it opens a new part, and the first line it writes on `book_events`
is `connect` or `connect_error`. A new part alone is not proof, since a frame that lands just after its hour's
file was closed opens one too. The connections still up before such a start are closed at the last line the old
process wrote, with a synthetic `disconnect` that goes through the venue's parser like a recorded one, so
coverage, `gap` rows and book state all follow from it.

When the new process starts in a later hour, the evidence is in that hour's first lines: they start with a
connect and none of the connections left open carries on. With no later file there is no evidence, and nothing
is inferred.
"""
from __future__ import annotations

from itertools import islice
from typing import Callable, Iterable, Iterator

from .storage import RawLine
from .venues.common import Out, as_int

STREAM = "book_events"
STARTS = ("connect", "connect_error")
LOOK_AHEAD = 5000  # lines of the next file in which a connection that is still alive will have spoken
REASON = "recorder restart (inferred)"


def with_disconnects(lines: Iterable[RawLine], out: Out,
                     following: Callable[[str], Iterable[RawLine]]) -> Iterator[RawLine]:
    """`lines` of one stream-hour plus the missing disconnects. `following(host)` is the host's next raw.

    Has to be consumed line by line: what is still open is read from `out.coverage` as the parser feeds it.
    """
    prev: RawLine | None = None
    for ln in lines:
        if prev is not None:
            if ln.host != prev.host:
                yield from _at_end(prev, out, following)
            elif ln.part != prev.part and ln.rec.get("ctl") in STARTS:
                # the rows take the connect's line number: a connect writes none of its own
                yield from _close(prev, ln.line, out.coverage.up_conns(ln.host), out)
        yield ln
        prev = ln
    if prev is not None:
        yield from _at_end(prev, out, following)


def _at_end(last: RawLine, out: Out, following) -> Iterator[RawLine]:
    still_up = out.coverage.up_conns(last.host)
    if still_up and _restarted(still_up, following(last.host)):
        yield from _close(last, last.line + 1, still_up, out)


def _restarted(still_up: list[int], later: Iterable[RawLine]) -> bool:
    started: set[int] = set()
    for i, ln in enumerate(islice(later, LOOK_AHEAD)):
        ctl, conn = ln.rec.get("ctl"), as_int(ln.rec.get("conn"))
        if i == 0 and ctl not in STARTS:
            return False
        if ctl in STARTS:
            started.add(conn)
        elif conn in still_up and conn not in started:
            return False  # an old connection carries on: this was an ordinary reconnect
    return bool(started)


def _close(last: RawLine, line: int, conns: list[int], out: Out) -> Iterator[RawLine]:
    item = 0  # the connections share a line number, so their rows continue one numbering
    for conn in conns:
        n = len(out.coverage.live(last.host, conn))
        out.stats["ctl.disconnect_inferred"] += 1
        yield RawLine(last.host, line, {"recv_ms": last.rec.get("recv_ms"), "conn": conn, "ctl": "disconnect",
                                        "reason": REASON, "item0": item}, last.part)
        item += n
