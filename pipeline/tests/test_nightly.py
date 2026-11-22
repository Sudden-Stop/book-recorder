import json
import shutil
import subprocess
from datetime import datetime, timezone

import pytest

from book_pipeline import chain_check, cli, curate, nightly

from conftest import DT, FIXTURES, HOUR


@pytest.fixture
def root(tmp_path):
    """One storage root holding raw/ (as in the bucket); curated/ and reports/ get written next to it."""
    dst = tmp_path / "bucket"
    shutil.copytree(FIXTURES / "raw", dst / "raw")
    return dst


def _quiet(_):
    pass


@pytest.fixture(autouse=True)
def no_rpc(monkeypatch):
    monkeypatch.delenv(chain_check.RPC_ENV, raising=False)  # no test may reach a real RPC


def _report(root, step, ok, **more):
    p = root / "reports" / step / "venue=polymarket" / f"dt={DT}" / f"hour-{HOUR}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"ok": ok, **more}))


def _summary(root):
    return json.loads((root / "reports" / "nightly" / f"dt={DT}.json").read_text())


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


def test_chain_runs_after_every_venue_and_cannot_cost_them_anything(root, monkeypatch):
    events = []

    def killed(root_uri, dt, hour):
        events.append({"step": "chain-start"})
        raise RuntimeError("chain check killed by signal 9 (out of memory?)")

    monkeypatch.setattr(nightly, "_chain_hour", killed)
    s = nightly.run(str(root), DT, chain=True, log=events.append)
    assert s["venues"]["cross"]["curated"] == [HOUR]
    start = events.index({"step": "chain-start"})
    assert any(e.get("venue") == "cross" and e.get("step") == "validate" for e in events[:start])
    assert not any(e.get("venue") == "cross" for e in events[start:])
    assert [(f["step"], f["hour"]) for f in s["failed"]] == [("chain", HOUR)]
    assert _summary(root)["finished_at"]


def test_resume_keeps_a_failed_chain_check(root, monkeypatch):
    nightly.run(str(root), DT, log=_quiet)
    (root / "reports" / "nightly" / f"dt={DT}.json").unlink()  # the run that checked it died before the summary
    _report(root, "chain", False, problems=["1 activity rows without an on-chain fill"])
    monkeypatch.setattr(nightly, "_chain_hour", lambda *a: pytest.fail("checked again"))
    for chain in (True, False):
        s = nightly.run(str(root), DT, chain=chain, log=_quiet)
        assert not s["ok"]
        assert s["failed"] == [{"venue": "polymarket", "hour": HOUR, "step": "chain",
                                "error": "1 activity rows without an on-chain fill"}]
        assert s["venues"]["polymarket"]["chain_skipped"] == [HOUR]


def test_resume_keeps_a_failed_validation(root):
    nightly.run(str(root), DT, log=_quiet)
    _report(root, "validate", False, schema_drift=["trades: missing column"])
    s = nightly.run(str(root), DT, log=_quiet)
    assert s["failed"] == [{"venue": "polymarket", "hour": HOUR, "step": "validate", "error": "trades: missing column"}]


def test_an_unreadable_report_is_made_again(root, monkeypatch):
    nightly.run(str(root), DT, log=_quiet)
    _report(root, "chain", True)
    for step in ("chain", "validate"):
        (root / "reports" / step / "venue=polymarket" / f"dt={DT}" / f"hour-{HOUR}.json").write_text("{half")
    monkeypatch.setattr(nightly, "_chain_hour", lambda *a: {"ok": True, "problems": []})
    s = nightly.run(str(root), DT, chain=True, log=_quiet)
    assert s["ok"], s["failed"]
    assert s["venues"]["polymarket"]["validated"] == s["venues"]["polymarket"]["chain_checked"] == [HOUR]


def test_a_rerun_of_some_venues_keeps_the_rest_of_the_summary(root, monkeypatch):
    def down(*a):
        raise RuntimeError("rpc down")  # leaves no chain report behind

    monkeypatch.setattr(nightly, "_chain_hour", down)
    nightly.run(str(root), DT, chain=True, log=_quiet)
    for kw in ({"venues": ("cross",)}, {"venues": ("polymarket",), "chain": False}):
        s = nightly.run(str(root), DT, log=_quiet, **kw)
        assert not s["ok"]
        assert [(f["venue"], f["step"]) for f in s["failed"]] == [("polymarket", "chain")]
        assert set(_summary(root)["venues"]) == {"polymarket", "kalshi", "cross"}
    monkeypatch.setattr(nightly, "_chain_hour", lambda *a: {"ok": True, "problems": []})
    assert nightly.run(str(root), DT, chain=True, log=_quiet)["ok"]  # and lets go of it once it passes


def test_force_checks_again(root, monkeypatch):
    nightly.run(str(root), DT, log=_quiet)
    _report(root, "chain", False, problems=["old"])
    monkeypatch.setattr(nightly, "_chain_hour", lambda *a: {"ok": True, "problems": []})
    assert nightly.run(str(root), DT, force=True, chain=True, log=_quiet)["ok"]


def test_chain_hour_reads_the_child_result(monkeypatch):
    url = "https://rpc.test/v2/not-a-real-key"
    monkeypatch.setenv(chain_check.RPC_ENV, url)

    def child(code, out="", err=""):
        monkeypatch.setattr(nightly.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, code, out, err))

    child(1, 'noise\n{"ok": false, "problems": ["x"]}\n')
    assert nightly._chain_hour("r", DT, HOUR) == {"ok": False, "problems": ["x"]}
    child(-9)
    with pytest.raises(RuntimeError, match="signal 9"):
        nightly._chain_hour("r", DT, HOUR)
    child(1, err=f"Traceback\nValueError: cannot reach {url}\n")
    with pytest.raises(RuntimeError, match="<rpc>") as e:
        nightly._chain_hour("r", DT, HOUR)
    assert "not-a-real-key" not in str(e.value)

    def hang(*a, **k):
        raise subprocess.TimeoutExpired(a, k["timeout"])

    monkeypatch.setattr(nightly.subprocess, "run", hang)
    with pytest.raises(RuntimeError, match="killed after"):
        nightly._chain_hour("r", DT, HOUR)


def test_chain_hour_starts_a_real_child(tmp_path):
    # an impossible date fails in the child before any network call
    with pytest.raises(RuntimeError, match="exited 1: ValueError"):
        nightly._chain_hour(str(tmp_path), "not-a-date", HOUR)
