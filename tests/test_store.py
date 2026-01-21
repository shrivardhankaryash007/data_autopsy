import json
import time
from pathlib import Path

from autopsy.store import MeasurementStore, sha256_file_signature


def test_fingerprint_changes_when_head_changes(tmp_path: Path):
    p = tmp_path / "a.bin"
    p.write_bytes(b"hello" * 100)

    f1 = sha256_file_signature(p, head_bytes=100)
    time.sleep(1)  # ensure mtime changes on some FS
    p.write_bytes(b"HELLO" * 100)
    f2 = sha256_file_signature(p, head_bytes=100)

    assert f1 != f2


def test_add_creates_meta_and_is_idempotent(tmp_path: Path):
    store = MeasurementStore(root=tmp_path / ".cache")
    p = tmp_path / "x.csv"
    p.write_text("a,b\n1,2\n", encoding="utf-8")

    ref1 = store.add(p, label="run1")
    meta_path = (tmp_path / ".cache" / "meta" / f"{ref1.id}.json")
    assert meta_path.exists()

    meta1 = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta1["measurement_id"] == ref1.id
    assert meta1["label"] == "run1"

    # add again (should not duplicate)
    ref2 = store.add(p, label="run1")
    assert ref1.id == ref2.id

    meta2 = store.meta(ref1.id)
    assert meta2["path"].endswith("x.csv")


def test_cache_path_is_deterministic(tmp_path: Path):
    store = MeasurementStore(root=tmp_path / ".cache")
    mid = "m_abc123"
    p1 = store.cache_path(mid, "overview", "v1_1hz", ".parquet")
    p2 = store.cache_path(mid, "overview", "v1_1hz", ".parquet")
    assert p1 == p2
    assert str(p1).endswith(".parquet")
