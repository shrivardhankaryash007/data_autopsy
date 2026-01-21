from pathlib import Path

import pytest

from autopsy.store import MeasurementStore

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")


def _write_pass1_csv(path: Path) -> None:
    rows = ["timestamp,signal_a,signal_b"]
    for bucket in range(60):
        base_time = bucket
        if 20 <= bucket <= 29:
            a_values = [5.0, 5.0]
        else:
            a_values = [float(bucket), float(bucket) + 0.5]
        if bucket == 50:
            b_values = [100.0, 100.0]
        else:
            b_values = [0.0, 0.0]
        rows.append(f"{base_time},{a_values[0]},{b_values[0]}")
        rows.append(f"{base_time + 0.5},{a_values[1]},{b_values[1]}")
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def test_pass1_windows_and_cache(tmp_path: Path) -> None:
    store = MeasurementStore(root=tmp_path / ".cache")
    csv_path = tmp_path / "pass1.csv"
    _write_pass1_csv(csv_path)
    ref = store.add(csv_path)

    overview_cfg = {
        "signals": ["signal_a", "signal_b"],
        "hz": 1.0,
        "agg": ["min", "mean", "max"],
        "time_col": "timestamp",
    }
    store.build_overview(ref.id, **overview_cfg)

    pass1_cfg = {
        "missing_rate": 0.1,
        "flatline_eps": 0.01,
        "flatline_min_run": 10,
        "spike_mad_z": 5.0,
        "top_k_windows": 5,
        "top_n_signals": 2,
    }

    first = store.run_autopsy_pass1(ref.id, overview_cfg, pass1_cfg)
    second = store.run_autopsy_pass1(ref.id, overview_cfg, pass1_cfg)

    assert first["key"] == second["key"]
    assert second["cache_hit"] is True

    windows = {(w["start_bucket"], w["end_bucket"]) for w in first["windows"]}
    assert (20, 29) in windows
    assert (50, 50) in windows
