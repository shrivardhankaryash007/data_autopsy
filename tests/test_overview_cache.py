import time
from pathlib import Path

import pytest

from autopsy.store import MeasurementStore

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")


def _write_csv(path: Path) -> None:
    path.write_text(
        "timestamp,signal_a,signal_b\n"
        "2024-01-01T00:00:00Z,1,10\n"
        "2024-01-01T00:00:00.500Z,2,20\n"
        "2024-01-01T00:00:01Z,3,30\n",
        encoding="utf-8",
    )


def test_overview_is_deterministic(tmp_path: Path) -> None:
    store = MeasurementStore(root=tmp_path / ".cache")
    csv_path = tmp_path / "sample.csv"
    _write_csv(csv_path)
    ref = store.add(csv_path)

    first = store.build_overview(ref.id, signals=["signal_a", "signal_b"], hz=1.0)
    second = store.build_overview(ref.id, signals=["signal_a", "signal_b"], hz=1.0)

    assert first["path"] == second["path"]
    assert second["cache_hit"] is True


def test_overview_cache_hit_preserves_mtime(tmp_path: Path) -> None:
    store = MeasurementStore(root=tmp_path / ".cache")
    csv_path = tmp_path / "sample.csv"
    _write_csv(csv_path)
    ref = store.add(csv_path)

    first = store.build_overview(ref.id, hz=1.0)
    overview_path = first["path"]
    initial_mtime = overview_path.stat().st_mtime

    time.sleep(1)
    second = store.build_overview(ref.id, hz=1.0)
    refreshed_mtime = overview_path.stat().st_mtime

    assert second["cache_hit"] is True
    assert refreshed_mtime == initial_mtime
