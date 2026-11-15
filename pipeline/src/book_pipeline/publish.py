"""publish: curated tables -> a Hugging Face dataset repo (one config per table), private by default.

Two steps, so that what would be uploaded can be inspected first:

1. `stage`: copy the curated Parquet files into `<stage>/data/<table>/<venue>/<dt>/part-HH.parquet` (no `key=`
   directory names, so the Hub does not add hive-partition columns that clash with the `venue` column), apply the
   wallet policy to columns marked `sensitive: wallet` in `schema/`, and render `<stage>/README.md` (the dataset
   card) from `docs/dataset_card_template.md`.
2. `push`: create the repo (private unless `public=True` is passed explicitly) and upload the staged folder.

Wallet policy: `raw` keeps addresses, `hashed` replaces each with hex(sha256(salt || lower(address))) using a
secret salt (env `BOOK_WALLET_SALT`, at least 16 characters, never written to the repo), `drop` removes the
column from the published files and the card.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from . import schema
from .storage import Root

WALLET_MODES = ("raw", "hashed", "drop")
_CURATED = re.compile(r"curated/(?P<table>[^/]+)/venue=(?P<venue>[^/]+)/dt=(?P<dt>[^/]+)/part-(?P<hour>\d\d)\.parquet$")


def template_path() -> Path:
    env = os.environ.get("BOOK_CARD_TEMPLATE")
    if env:
        return Path(env)
    return Path(__file__).resolve().parents[3] / "docs" / "dataset_card_template.md"


def _hash_wallets(arr: pa.ChunkedArray, salt: str) -> pa.Array:
    out = []
    for v in arr.to_pylist():
        if v is None:
            out.append(None)
        else:
            out.append(hashlib.sha256((salt + v.lower()).encode()).hexdigest())
    return pa.array(out, pa.string())


def _apply_wallets(t: pa.Table, table: schema.Table, mode: str, salt: str | None) -> pa.Table:
    for col in table.columns:
        if col.sensitive != "wallet" or col.name not in t.column_names:
            continue
        if mode == "drop":
            t = t.drop_columns([col.name])
        elif mode == "hashed":
            idx = t.column_names.index(col.name)
            t = t.set_column(idx, t.schema.field(col.name), _hash_wallets(t.column(col.name), salt))
    return t


def stage(curated_root: str, stage_dir: str | Path, wallets: str = "hashed", salt: str | None = None,
          tables: tuple[str, ...] = schema.TABLES, venues: tuple[str, ...] | None = None,
          dt_from: str | None = None, dt_to: str | None = None, pretty_name: str = "Prediction market order books",
          ) -> dict:
    if wallets not in WALLET_MODES:
        raise ValueError(f"wallets must be one of {WALLET_MODES}")
    if wallets == "hashed":
        salt = salt or os.environ.get("BOOK_WALLET_SALT")
        if not salt or len(salt) < 16:
            raise ValueError("wallets='hashed' needs a salt of at least 16 characters (BOOK_WALLET_SALT)")
    root = Root.parse(curated_root)
    stage_dir = Path(stage_dir)
    defs = schema.load_all()
    summary: dict[str, dict] = {}
    venues_seen, dts = set(), set()
    for name in tables:
        table = defs[name]
        info = {"files": 0, "rows": 0, "dt_min": None, "dt_max": None}
        for path in root.glob_dirs(f"curated/{name}"):
            m = _CURATED.search(path)
            if not m:
                continue
            v, dt, hour = m["venue"], m["dt"], m["hour"]
            if venues and v not in venues or dt_from and dt < dt_from or dt_to and dt > dt_to:
                continue
            with root.fs.open_input_file(path) as f:
                t = pq.read_table(f)
            drift = schema.diff(table, t.schema)
            if drift:
                raise schema.SchemaDrift(f"{path}: {drift}")
            t = _apply_wallets(t, table, wallets, salt)
            out = stage_dir / "data" / name / v / dt / f"part-{hour}.parquet"
            out.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(t, out, compression="zstd", compression_level=9)
            info["files"] += 1
            info["rows"] += t.num_rows
            info["dt_min"] = min(filter(None, [info["dt_min"], dt]))
            info["dt_max"] = max(filter(None, [info["dt_max"], dt]))
            venues_seen.add(v)
            dts.add(dt)
        summary[name] = info
    reports = _validate_reports(root, venues, dt_from, dt_to)
    card = render_card(summary, wallets, sorted(venues_seen), sorted(dts), reports, pretty_name)
    (stage_dir / "README.md").write_text(card, encoding="utf-8")
    manifest = {"tables": summary, "wallets": wallets, "venues": sorted(venues_seen),
                "dt_min": min(dts) if dts else None, "dt_max": max(dts) if dts else None}
    return manifest


def _validate_reports(root: Root, venues, dt_from, dt_to) -> list[dict]:
    out = []
    for path in root.glob_dirs("reports/validate"):
        m = re.search(r"venue=([^/]+)/dt=([^/]+)/hour-\d\d\.json$", path)
        if not m or venues and m[1] not in venues or dt_from and m[2] < dt_from or dt_to and m[2] > dt_to:
            continue
        with root.fs.open_input_stream(path, compression=None) as f:
            out.append(json.loads(f.read()))
    return out


def _md_escape(s: str) -> str:
    return s.replace("|", "\\|").replace("\n", " ")


def render_card(summary: dict, wallets: str, venues: list[str], dts: list[str], reports: list[dict],
                pretty_name: str) -> str:
    defs = schema.load_all()
    names = [n for n in schema.TABLES if n in summary]
    front = {
        "pretty_name": pretty_name,
        "license": "other",
        "tags": ["finance", "prediction-markets", "order-book", "market-microstructure"],
        "configs": [{"config_name": n, "data_files": [{"split": "train", "path": f"data/{n}/**/*.parquet"}],
                     **({"default": True} if i == 0 else {})} for i, n in enumerate(names)],
    }
    rows = ["| Config | Files | Rows | Dates | What it is |", "|---|---:|---:|---|---|"]
    for n in names:
        s = summary[n]
        dates = f"{s['dt_min']} – {s['dt_max']}" if s["dt_min"] else "—"
        first = defs[n].description.split(". ")[0].rstrip(".") + "."
        rows.append(f"| `{n}` | {s['files']} | {s['rows']:,} | {dates} | {_md_escape(first)} |")
    sections = []
    for n in names:
        t = defs[n]
        lines = [f"### `{n}`", "", t.description, ""]
        if t.order:
            lines += [f"Row order within a file: {', '.join(f'`{c}`' for c in t.order)}.", ""]
        lines += ["| Column | Type | Nullable | Meaning |", "|---|---|---|---|"]
        for c in t.columns:
            if c.sensitive == "wallet" and wallets == "drop":
                continue
            meaning = c.meaning
            if c.sensitive == "wallet" and wallets == "hashed":
                meaning += " **In this release: salted SHA-256 hex digest, stable across the release.**"
            lines.append(f"| `{c.name}` | `{c.type}` | {'yes' if c.nullable else 'no'} | {_md_escape(meaning)} |")
        sections.append("\n".join(lines))
    policy = {"raw": "wallet addresses are published as recorded (lower-case).",
              "hashed": "wallet addresses are replaced by a salted SHA-256 digest; the salt is secret, so digests "
                        "link trades of the same wallet within this release but cannot be reversed by lookup.",
              "drop": "wallet addresses are removed from the published files."}[wallets]
    values = {
        "pretty_name": pretty_name,
        "venues": ", ".join(venues) if venues else "none",
        "date_range": f"{dts[0]} to {dts[-1]}" if dts else "no data",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "wallet_policy": policy,
        "summary_table": "\n".join(rows),
        "tables": "\n\n".join(sections),
        "replay": _replay_text(reports),
        "coverage": _coverage_text(reports),
        "schema_types": _types_text(),
    }
    body = template_path().read_text(encoding="utf-8")
    for k, v in values.items():
        body = body.replace("{{" + k + "}}", v)
    left = re.findall(r"\{\{(\w+)\}\}", body)
    if left:
        raise ValueError(f"dataset card template has unknown placeholders: {left}")
    return "---\n" + yaml.safe_dump(front, sort_keys=False, allow_unicode=True) + "---\n\n" + body


def _replay_text(reports: list[dict]) -> str:
    tot = {"exact": 0, "window": 0, "top": 0, "mismatch": 0, "skipped": 0}
    hours = 0
    for r in reports:
        rp = r.get("replay")
        if not rp:
            continue
        hours += 1
        for k in tot:
            tot[k] += rp["outcomes"].get(k, 0)
    ev = tot["exact"] + tot["window"] + tot["top"] + tot["mismatch"]
    if not ev:
        return "_No validation reports were found for this release._"
    return (f"Over {hours} validated hour(s), {ev:,} snapshot intervals were evaluated: "
            f"{tot['exact']:,} exact ({tot['exact'] / ev:.2%}), {tot['window']:,} within the timing window, "
            f"{tot['top']:,} with only the top of book matching, {tot['mismatch']:,} mismatched; "
            f"{tot['skipped']:,} intervals were skipped because of connection gaps.")


def _coverage_text(reports: list[dict]) -> str:
    sub = con = 0.0
    mh = gaps = 0
    for r in reports:
        c = r.get("coverage")
        if not c:
            continue
        sub += c.get("subscribed_s") or 0
        con += c.get("connected_s") or 0
        mh += c.get("market_hours") or 0
        gaps += c.get("market_hours_with_gaps") or 0
    if not mh:
        return "_No coverage summary was found for this release._"
    return (f"{mh:,} market-hours; connected {con / sub:.2%} of subscribed time; "
            f"{gaps:,} market-hours contain at least one gap.")


def _types_text() -> str:
    return ("| Type | Parquet |\n|---|---|\n| `price` | decimal(10, 6) |\n| `size` | decimal(24, 6) |\n"
            "| `timestamp` | timestamp, milliseconds, UTC |\n| `levels` | list of struct(price, size), best first |\n"
            "| `tokens` | list of struct(token_id, outcome) |\n| `list<string>` | list of string |\n"
            "| `string`, `bool`, `int32`, `int64`, `float64` | same |")


def push(stage_dir: str | Path, repo_id: str, public: bool = False, token: str | None = None, api=None,
         commit_message: str | None = None) -> str:
    """Upload a staged folder. Private unless `public=True`; an existing repo's visibility is not changed."""
    if api is None:
        from huggingface_hub import HfApi
        api = HfApi(token=token or os.environ.get("HF_TOKEN"))
    api.create_repo(repo_id, repo_type="dataset", private=not public, exist_ok=True)
    info = api.upload_folder(folder_path=str(stage_dir), repo_id=repo_id, repo_type="dataset",
                             commit_message=commit_message or "Update dataset from book-pipeline")
    return str(getattr(info, "commit_url", info))
