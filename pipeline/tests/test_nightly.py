import json
import shutil
from datetime import datetime, timezone

import pytest

from book_pipeline import cli, curate, nightly

from conftest import DT, FIXTURES, HOUR


@pytest.fixture
def root(tmp_path):
    """One storage root holding raw/ (as in the bucket); curated/ and reports/ get written next to it."""
    dst = tmp_path / "bucket"
    shutil.copytree(FIXTURES / "raw", dst / "raw")
    return dst


def _quiet(_):
    pass


def test_recorded_hours_finds_only_hours_with_raw(root):
    r = nightly.Root.parse(str(root))
    assert nightly.recorded_hours(r, "polymarket", DT) == [HOUR]
    assert nightly.recorded_hours(r, "cross", DT) == [HOUR]
    assert nightly.recorded_hours(r, "kalshi", DT) == []


def test_curates_and_validates_every_recorded_hour(root):
    s = nightly.run(str(root), DT, log=_quiet)
    assert s["ok"], s["failed"]
    assert s["venues"]["polymarket"]["curated"] == [HOUR]
    assert s["venues"]["polymarket"]["validated"] == [HOUR]
    assert s["venues"]["cross"]["curated"] == [HOUR]
    assert s["venues"]["kalshi"]["hours"] == []
    assert (root / "curated" / "book_deltas" / "venue=polymarket" / f"dt={DT}" / f"part-{HOUR}.parquet").exists()
    assert (root / "reports" / "validate" / "venue=polymarket" / f"dt={DT}" / f"hour-{HOUR}.json").exists()
    saved = json.loads((root / "reports" / "nightly" / f"dt={DT}.json").read_text())
    assert saved["ok"] and saved["dt"] == DT


def test_rerun_skips_done_hours_and_force_redoes_them(root):
    nightly.run(str(root), DT, venues=("polymarket",), log=_quiet)
    again = nightly.run(str(root), DT, venues=("polymarket",), log=_quiet)
    assert again["venues"]["polymarket"]["skipped"] == [HOUR]
    assert again["venues"]["polymarket"]["curated"] == []
    assert again["venues"]["polymarket"]["validated"] == []
    forced = nightly.run(str(root), DT, venues=("polymarket",), force=True, log=_quiet)
    assert forced["venues"]["polymarket"]["curated"] == [HOUR]
    assert forced["venues"]["polymarket"]["validated"] == [HOUR]


def test_a_failing_hour_is_recorded_and_the_rest_still_runs(root, monkeypatch):
    real = curate.run

    def flaky(raw, out, venue, dt, hour, **kw):
        if venue == "polymarket":
            raise RuntimeError("boom")
        return real(raw, out, venue, dt, hour, **kw)

    monkeypatch.setattr(nightly.curate, "run", flaky)
    s = nightly.run(str(root), DT, log=_quiet)
    assert not s["ok"]
    assert s["failed"] == [{"venue": "polymarket", "hour": HOUR, "step": "curate", "error": "RuntimeError: boom"}]
    assert s["venues"]["cross"]["curated"] == [HOUR]


def test_cli_exit_codes(root, monkeypatch, capsys):
    assert cli.main(["nightly", "--root", str(root), "--dt", DT]) == 0
    assert '"ok": true' in capsys.readouterr().out.splitlines()[-1]
    monkeypatch.setattr(nightly.curate, "run", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    assert cli.main(["nightly", "--root", str(root), "--dt", DT, "--force", "--venue", "polymarket"]) == 1


def test_default_root_and_date(monkeypatch):
    monkeypatch.delenv("BOOK_ROOT", raising=False)
    monkeypatch.setenv("BR_S3_BUCKET", "my-bucket")
    assert nightly.default_root() == "s3://my-bucket"
    monkeypatch.setenv("BOOK_ROOT", "/data/root")
    assert nightly.default_root() == "/data/root"
    assert nightly.yesterday_utc(datetime(2026, 3, 1, 2, 30, tzinfo=timezone.utc)) == "2026-02-28"
