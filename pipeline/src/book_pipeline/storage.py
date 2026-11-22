"""Local directories and S3 prefixes behind one interface (pyarrow.fs), plus raw-file iteration."""
from __future__ import annotations

import gzip
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

import orjson
import pyarrow.fs as pafs
import zstandard

RAW_SUFFIXES = (".ndjson.zst", ".ndjson.gz", ".ndjson")
_PART = re.compile(r"^(?P<host>.+?)(?:[-.]p(?P<part>\d+))?$")


@dataclass(frozen=True)
class Root:
    """A storage root: `s3://bucket/prefix` or a local directory."""
    fs: pafs.FileSystem
    base: str

    @classmethod
    def parse(cls, uri: str | Path) -> "Root":
        s = str(uri)
        if "://" in s:
            fs, base = pafs.FileSystem.from_uri(s)
            return cls(fs, base.rstrip("/"))
        return cls(pafs.LocalFileSystem(), Path(s).resolve().as_posix().rstrip("/"))

    def join(self, *parts: str) -> str:
        return "/".join([self.base, *parts])

    def list(self, rel: str) -> list[str]:
        sel = pafs.FileSelector(self.join(rel), allow_not_found=True, recursive=False)
        return sorted(i.path for i in self.fs.get_file_info(sel) if i.type == pafs.FileType.File)

    def glob_dirs(self, rel: str) -> list[str]:
        sel = pafs.FileSelector(self.join(rel), allow_not_found=True, recursive=True)
        return sorted(i.path for i in self.fs.get_file_info(sel) if i.type == pafs.FileType.File)

    def exists(self, path: str) -> bool:
        return self.fs.get_file_info(path).type != pafs.FileType.NotFound

    def makedirs(self, path: str) -> None:
        self.fs.create_dir(path.rsplit("/", 1)[0], recursive=True)


def raw_dir(venue: str, stream: str, dt: str, hour: str) -> str:
    return f"raw/venue={venue}/stream={stream}/dt={dt}/hour={hour}"


def host_of(path: str) -> str:
    name = path.rsplit("/", 1)[-1]
    for suf in RAW_SUFFIXES:
        if name.endswith(suf):
            name = name[: -len(suf)]
            break
    m = _PART.match(name)
    return m.group("host") if m else name


def _part_of(path: str) -> int:
    name = path.rsplit("/", 1)[-1]
    for suf in RAW_SUFFIXES:
        if name.endswith(suf):
            name = name[: -len(suf)]
    m = _PART.match(name)
    return int(m.group("part")) if m and m.group("part") else 0


@dataclass
class ReadStats:
    files: int = 0
    lines: int = 0
    bad_json: int = 0
    layout: str | None = None
    truncated_files: list[str] = field(default_factory=list)
    truncated_tail_bytes: int = 0
    by_host: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class RawLine:
    host: str
    line: int
    rec: dict
    part: int = 0


def local_dir(venue: str, stream: str, dt: str) -> str:
    """The recorder's own data directory layout: `{venue}/{stream}/dt=YYYY-MM-DD/HH.pN.ndjson.zst`."""
    return f"{venue}/{stream}/dt={dt}"


_LOCAL = re.compile(r"^(?P<hour>\d\d)\.p(?P<part>\d+)\.ndjson(\.zst|\.gz)?$")


def raw_files(root: Root, venue: str, stream: str, dt: str, hour: str, local_host: str = "local"
              ) -> tuple[str | None, list[tuple[str, int, str]]]:
    """(layout, [(host, part, path)]) for one stream-hour.

    The S3 layout (`raw/venue=…/stream=…/dt=…/hour=HH/<host>-pN.ndjson.zst`) is tried first; if it has no files,
    the recorder's local data-dir layout is tried, with every file attributed to `local_host`.
    `.manifest.json` siblings are ignored.
    """
    files = [p for p in root.list(raw_dir(venue, stream, dt, hour)) if p.endswith(RAW_SUFFIXES)]
    if files:
        return "s3", sorted(((host_of(p), _part_of(p), p) for p in files), key=lambda x: (x[0], x[1], x[2]))
    out = []
    for p in root.list(local_dir(venue, stream, dt)):
        m = _LOCAL.match(p.rsplit("/", 1)[-1])
        if m and m["hour"] == f"{int(hour):02d}":
            out.append((local_host, int(m["part"]), p))
    return ("local" if out else None), sorted(out, key=lambda x: (x[1], x[2]))


def iter_raw(root: Root, venue: str, stream: str, dt: str, hour: str, stats: ReadStats | None = None,
             local_host: str = "local", only_host: str | None = None) -> Iterator[RawLine]:
    """Yield the records of one stream-hour, host by host, parts in order, numbering lines per host.

    Reads streaming (one decompressed chunk at a time), so memory does not grow with the file size. A part cut
    short by a crash (no zstd end marker) yields every complete line before the cut; the incomplete tail is
    dropped and counted (`truncated_files`, `truncated_tail_bytes`).
    """
    stats = stats if stats is not None else ReadStats()
    layout, files = raw_files(root, venue, stream, dt, hour, local_host)
    stats.layout = layout
    counters: dict[str, int] = {}
    for host, part, path in files:
        if only_host is not None and host != only_host:
            continue
        stats.files += 1
        with root.fs.open_input_stream(path, compression=None) as raw:
            for buf in _lines(raw, path, stats):
                if not buf.strip():
                    continue
                n = counters.get(host, 0)
                counters[host] = n + 1
                stats.lines += 1
                try:
                    rec = orjson.loads(buf)
                except orjson.JSONDecodeError:
                    stats.bad_json += 1
                    continue
                if isinstance(rec, dict):
                    yield RawLine(host, n, rec, part)
                else:
                    stats.bad_json += 1
    stats.by_host = counters


CHUNK = 1 << 20


def _zstd_chunks(raw, path: str, stats: ReadStats) -> Iterator[bytes]:
    """Decompress frame after frame; stop quietly (and record it) at a truncated or corrupt tail."""
    dctx = zstandard.ZstdDecompressor()
    obj = dctx.decompressobj()
    in_frame = False
    while True:
        data = raw.read(CHUNK)
        if not data:
            break
        while data:
            try:
                out = obj.decompress(data)
            except zstandard.ZstdError:
                stats.truncated_files.append(path)
                return
            in_frame = True
            if out:
                yield out
            if obj.eof:
                data = obj.unused_data
                obj = dctx.decompressobj()
                in_frame = False
            else:
                data = b""
    if in_frame:
        stats.truncated_files.append(path)


def _lines(raw, path: str, stats: ReadStats) -> Iterator[bytes]:
    if path.endswith(".zst"):
        pending = b""
        for chunk in _zstd_chunks(raw, path, stats):
            pending += chunk
            lines = pending.split(b"\n")
            pending = lines.pop()
            yield from lines
        if pending.strip():
            if stats.truncated_files and stats.truncated_files[-1] == path:
                stats.truncated_tail_bytes += len(pending)  # half-written last line of a crashed part
            else:
                yield pending
        return
    stream = gzip.GzipFile(fileobj=raw) if path.endswith(".gz") else raw
    for line in stream:
        yield line


def frame_of(rec: dict):
    """The venue frame of a raw line: `msg` parsed if it was stored as a string, else the record itself."""
    if "msg" not in rec:
        return rec
    msg = rec["msg"]
    if isinstance(msg, str):
        try:
            return orjson.loads(msg)
        except orjson.JSONDecodeError:
            return None
    return msg
