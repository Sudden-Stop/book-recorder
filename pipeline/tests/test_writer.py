import pyarrow.fs as pafs
import pyarrow.parquet as pq

from book_pipeline import schema
from book_pipeline.storage import Root
from book_pipeline.writer import TableWriter


def _write(root: Root, path: str) -> int:
    return TableWriter(schema.load("pairs"), root, path).close()  # an empty table still produces a file


def test_local_root_stages_through_tmp_and_replaces(tmp_path):
    root = Root.parse(tmp_path)
    path = root.join("curated", "pairs", "part-00.parquet")
    _write(root, path)
    _write(root, path)  # rerun over an existing file
    assert pq.read_table(path).num_rows == 0
    assert [p.name for p in (tmp_path / "curated" / "pairs").iterdir()] == ["part-00.parquet"]


def test_object_store_root_writes_final_key_without_move_or_delete(tmp_path, monkeypatch):
    # Any non-local filesystem stands in for S3 (where a move is copy + delete and the role cannot delete).
    fs = pafs.SubTreeFileSystem(str(tmp_path), pafs.LocalFileSystem())
    assert fs.type_name != "local"
    root = Root(fs, "bucket")
    path = root.join("curated", "pairs", "part-00.parquet")

    checked = []
    monkeypatch.setattr(Root, "exists", lambda self, p: checked.append(p) or False)

    _write(root, path)
    _write(root, path)  # a rerun simply overwrites the key

    out = tmp_path / "bucket" / "curated" / "pairs"
    assert [p.name for p in out.iterdir()] == ["part-00.parquet"], "no .tmp object on an object store"
    assert checked == [], "no existence check (and so no delete or move) on an object store"
    assert pq.read_table(str(out / "part-00.parquet")).num_rows == 0
