from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from book_pipeline import schema, validate
from book_pipeline.curate import curated_path
from book_pipeline.storage import Root

from conftest import DT, HOUR


def test_every_table_has_a_yaml_with_meanings():
    tables = schema.load_all()
    assert set(tables) == set(schema.TABLES)
    for t in tables.values():
        assert t.description
        assert all(c.meaning for c in t.columns)
        assert t.names[0] == "venue"


def test_unknown_type_in_yaml_is_rejected(tmp_path):
    (tmp_path / "trades.yaml").write_text(
        "table: trades\ncolumns:\n  - {name: venue, type: varchar, meaning: x}\n", encoding="utf-8")
    with pytest.raises(schema.SchemaDrift):
        schema.load("trades", str(tmp_path))


@pytest.mark.parametrize("table", schema.TABLES)
def test_curated_files_match_declared_schema(curated, table):
    root = Root.parse(curated)
    path = curated_path(root, table, "cross" if table == "pairs" else "polymarket", DT, HOUR)
    actual = pq.read_schema(path)
    assert schema.diff(schema.load(table), actual) == []


def test_prices_are_decimals_and_times_are_utc_ms(curated):
    t = pq.read_table(curated_path(Root.parse(curated), "book_deltas", "polymarket", DT, HOUR))
    assert t.schema.field("price").type == pa.decimal128(10, 6)
    assert t.schema.field("size").type == pa.decimal128(24, 6)
    ts = t.schema.field("recv_ts").type
    assert ts.unit == "ms" and ts.tz == "UTC"
    row = t.filter(pa.compute.equal(t["action"], "set")).slice(0, 1).to_pylist()[0]
    assert isinstance(row["price"], Decimal)
    assert row["recv_ts"].tzinfo is not None


def test_drift_is_detected_and_fails_validate(curated_copy):
    root = Root.parse(curated_copy)
    path = curated_path(root, "trades", "polymarket", DT, HOUR)
    t = pq.read_table(path)
    t = t.set_column(t.column_names.index("price"), "price", t["price"].cast(pa.float64()))
    t = t.append_column("surprise", pa.array([1] * t.num_rows))
    pq.write_table(t, path)
    problems = validate.drift(root, [validate.Hour("polymarket", DT, HOUR)])
    assert any("trades.price: type double" in p for p in problems)
    assert any("unexpected column surprise" in p for p in problems)
    with pytest.raises(validate.ValidationFailed):
        validate.run(str(curated_copy), [validate.Hour("polymarket", DT, HOUR)], write_report=False)


def test_missing_table_file_is_drift(curated_copy):
    root = Root.parse(curated_copy)
    (curated_copy / "curated" / "markets").rename(curated_copy / "markets_moved")
    problems = validate.drift(root, [validate.Hour("polymarket", DT, HOUR)])
    assert any(p.startswith("markets: missing") for p in problems)
    assert not any("pairs" in p for p in problems)  # pairs belong to venue `cross`
    assert validate.drift(root, [validate.Hour("cross", DT, HOUR)]) == []
