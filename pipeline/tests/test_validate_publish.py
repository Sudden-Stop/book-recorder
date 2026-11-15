import hashlib
import json

import pyarrow.parquet as pq
import pytest
import yaml

from book_pipeline import cli, publish, validate

from conftest import DT, HOUR

HOURS = [validate.Hour("polymarket", DT, HOUR)]
SALT = "test-salt-0123456789"


def test_validate_passes_on_fixture(curated_copy):
    rep = validate.run(str(curated_copy), HOURS, min_match_rate=0.99, strict=True)
    assert rep["ok"] and rep["schema_drift"] == [] and rep["raw_drift"] == []
    assert rep["replay"]["match_rate"] == 1.0
    assert rep["coverage"]["market_hours"] == 4
    written = curated_copy / "reports" / "validate" / "venue=polymarket" / f"dt={DT}" / f"hour-{HOUR}.json"
    assert json.loads(written.read_text())["ok"] is True


def test_strict_fails_on_raw_drift(curated_copy):
    rp = curated_copy / "reports" / "curate" / "venue=polymarket" / f"dt={DT}" / f"hour-{HOUR}.json"
    rep = json.loads(rp.read_text())
    rep["unknown_event_types"] = {"new_thing": 3}
    rp.write_text(json.dumps(rep))
    validate.run(str(curated_copy), HOURS)  # not strict: only reported
    with pytest.raises(validate.ValidationFailed) as e:
        validate.run(str(curated_copy), HOURS, strict=True)
    assert "new_thing" in str(e.value)


def test_cli_validate_exit_code(curated_copy, capsys):
    assert cli.main(["validate", "--curated", str(curated_copy), "--venue", "polymarket", "--dt", DT]) == 0
    assert '"ok": true' in capsys.readouterr().out


def _stage(curated, tmp_path, wallets, **kw):
    validate.run(str(curated), HOURS)  # writes the validate report the card summarises
    out = tmp_path / f"stage-{wallets}"
    return out, publish.stage(str(curated), out, wallets=wallets, salt=SALT if wallets == "hashed" else None, **kw)


def test_stage_hashed_wallets_and_card(curated_copy, tmp_path):
    out, manifest = _stage(curated_copy, tmp_path, "hashed")
    assert manifest["venues"] == ["cross", "polymarket"] and manifest["tables"]["pairs"]["rows"] == 2 and manifest["tables"]["trades"]["files"] == 1
    trades = pq.read_table(out / "data" / "trades" / "polymarket" / DT / f"part-{HOUR}.parquet").to_pylist()
    raw = pq.read_table(curated_copy / "curated" / "trades" / "venue=polymarket" / f"dt={DT}" /
                        f"part-{HOUR}.parquet").to_pylist()
    for a, b in zip(raw, trades):
        if a["wallet"] is None:
            assert b["wallet"] is None
        else:
            assert b["wallet"] == hashlib.sha256((SALT + a["wallet"]).encode()).hexdigest()
    card = (out / "README.md").read_text(encoding="utf-8")
    front = yaml.safe_load(card.split("---\n")[1])
    assert [c["config_name"] for c in front["configs"]] == list(publish.schema.TABLES)
    assert front["configs"][0]["data_files"][0]["path"] == "data/book_deltas/**/*.parquet"
    assert "salted SHA-256" in card
    assert "{{" not in card
    assert "430 snapshot intervals" in card or "430 snapshot" in card
    assert "| `wallet` |" in card


def test_stage_drop_removes_wallet_column(curated_copy, tmp_path):
    out, _ = _stage(curated_copy, tmp_path, "drop")
    t = pq.read_table(out / "data" / "trades" / "polymarket" / DT / f"part-{HOUR}.parquet")
    assert "wallet" not in t.column_names
    assert "| `wallet` |" not in (out / "README.md").read_text(encoding="utf-8")


def test_stage_raw_keeps_wallets(curated_copy, tmp_path):
    out, _ = _stage(curated_copy, tmp_path, "raw")
    t = pq.read_table(out / "data" / "trades" / "polymarket" / DT / f"part-{HOUR}.parquet").to_pylist()
    assert any(r["wallet"] and r["wallet"].startswith("0x") for r in t)


def test_hashed_needs_a_salt(curated_copy, tmp_path, monkeypatch):
    monkeypatch.delenv("BOOK_WALLET_SALT", raising=False)
    with pytest.raises(ValueError):
        publish.stage(str(curated_copy), tmp_path / "s", wallets="hashed")


class FakeApi:
    def __init__(self):
        self.calls = []

    def create_repo(self, repo_id, **kw):
        self.calls.append(("create_repo", repo_id, kw))

    def upload_folder(self, **kw):
        self.calls.append(("upload_folder", kw))
        return "https://huggingface.co/datasets/x/y/commit/abc"


def test_push_is_private_by_default(tmp_path):
    api = FakeApi()
    publish.push(tmp_path, "org/books", api=api)
    assert api.calls[0] == ("create_repo", "org/books", {"repo_type": "dataset", "private": True, "exist_ok": True})
    assert api.calls[1][1]["repo_type"] == "dataset"
    api2 = FakeApi()
    publish.push(tmp_path, "org/books", public=True, api=api2)
    assert api2.calls[0][2]["private"] is False


def test_cli_publish_without_repo_only_stages(curated_copy, tmp_path, monkeypatch):
    called = []
    monkeypatch.setattr(publish, "push", lambda *a, **k: called.append(a))
    rc = cli.main(["publish", "--curated", str(curated_copy), "--stage", str(tmp_path / "st"), "--wallets", "drop"])
    assert rc == 0 and not called and (tmp_path / "st" / "README.md").exists()
