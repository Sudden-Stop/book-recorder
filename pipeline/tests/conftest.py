import json
import shutil
from pathlib import Path

import pytest
import zstandard

from book_pipeline import curate

FIXTURES = Path(__file__).parent / "fixtures"
DT, HOUR = "2026-11-14", "07"
H0 = curate.hour_start_ms(DT, HOUR)


def write_raw(root: Path, venue: str, stream: str, lines: list[dict], dt: str = DT, hour: str = HOUR,
              host: str = "rec-a", part: int = 0) -> Path:
    d = root / "raw" / f"venue={venue}" / f"stream={stream}" / f"dt={dt}" / f"hour={hour}"
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{host}-p{part}.ndjson.zst"
    data = "".join(json.dumps(r) + "\n" for r in lines).encode()
    p.write_bytes(zstandard.ZstdCompressor().compress(data))
    return p


@pytest.fixture(scope="session")
def curated(tmp_path_factory) -> Path:
    """The fixture hour curated once (small batch size so several row groups get written)."""
    out = tmp_path_factory.mktemp("curated")
    report = curate.run(str(FIXTURES), str(out), "polymarket", DT, HOUR, batch_rows=1000)
    (out / "_report.json").write_text(json.dumps(report))
    cross = curate.run(str(FIXTURES), str(out), "cross", DT, HOUR)
    (out / "_report_cross.json").write_text(json.dumps(cross))
    return out


@pytest.fixture
def curated_copy(curated, tmp_path) -> Path:
    dst = tmp_path / "copy"
    shutil.copytree(curated, dst)
    return dst
