from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List


@dataclass(frozen=True)
class AutopsyResultPass1:
    measurement_id: str
    overview_cfg: Dict[str, Any]
    pass1_cfg: Dict[str, Any]
    key: str
    created_at_unix: float
    per_signal: Dict[str, Dict[str, Any]]
    timestamp_checks: Dict[str, Any]
    windows: List[Dict[str, Any]]
    cache_hit: bool = False

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)


def _format_time(value: Any) -> Any:
    import pandas as pd

    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            return value.isoformat()
        return value.isoformat()
    return float(value)


def _flatline_mask(
    spread: Iterable[float],
    min_run: int,
) -> List[bool]:
    mask = [val for val in spread]
    run_mask = [False] * len(mask)
    runs: List[Dict[str, int]] = []
    start = None
    length = 0
    for idx, value in enumerate(mask):
        if value:
            if start is None:
                start = idx
                length = 1
            else:
                length += 1
        elif start is not None:
            runs.append({"start": start, "end": idx - 1, "length": length})
            start = None
            length = 0
    if start is not None:
        runs.append({"start": start, "end": len(mask) - 1, "length": length})

    for run in runs:
        if run["length"] >= min_run:
            for idx in range(run["start"], run["end"] + 1):
                run_mask[idx] = True
    return run_mask


def _mad_z(values: Iterable[float]) -> List[float]:
    import numpy as np

    vals = np.asarray(list(values), dtype="float64")
    if vals.size == 0:
        return []
    median = np.nanmedian(vals)
    mad = np.nanmedian(np.abs(vals - median))
    if mad == 0 or not np.isfinite(mad):
        return [0.0 for _ in vals]
    return list(0.6745 * (vals - median) / mad)


def build_pass1_from_overview(
    overview_df: Any,
    measurement_id: str,
    overview_cfg: Dict[str, Any],
    pass1_cfg: Dict[str, Any],
    key: str,
    cache_hit: bool,
) -> AutopsyResultPass1:
    import numpy as np
    import pandas as pd

    time_col = overview_cfg.get("time_col", "timestamp")
    signals = overview_cfg.get("signals")
    agg = overview_cfg.get("agg", ["min", "mean", "max"])
    if signals is None:
        signals = sorted(
            {
                col.rsplit("_", 1)[0]
                for col in overview_df.columns
                if "_" in col and col.split("_")[-1] in agg
            }
        )

    hz = float(overview_cfg.get("hz", 1.0))
    overview = overview_df.sort_values("bucket").reset_index(drop=True)

    time_values = overview[time_col]
    if pd.api.types.is_datetime64_any_dtype(time_values):
        seconds = time_values.astype("int64") / 1_000_000_000
    else:
        seconds = pd.to_numeric(time_values, errors="coerce")

    expected_gap = 1.0 / hz
    diffs = seconds.diff().to_numpy()
    monotonic = np.all(np.nan_to_num(diffs[1:], nan=expected_gap) >= 0)
    gap_mask = diffs > (expected_gap * 1.5)
    gap_indices = np.where(gap_mask)[0].tolist()

    timestamp_checks = {
        "monotonic": bool(monotonic),
        "gap_count": int(len(gap_indices)),
        "gap_indices": gap_indices,
        "expected_gap_seconds": expected_gap,
    }

    per_signal: Dict[str, Dict[str, Any]] = {}
    flagged_by_signal: Dict[str, List[bool]] = {}
    bucket_values = overview["bucket"].to_list()

    for signal in signals:
        mean_col = f"{signal}_mean"
        min_col = f"{signal}_min"
        max_col = f"{signal}_max"
        if mean_col not in overview.columns:
            raise KeyError(f"Missing {mean_col} in overview for {signal}")
        if min_col not in overview.columns or max_col not in overview.columns:
            raise KeyError(f"Missing {min_col}/{max_col} in overview for {signal}")

        mean_values = overview[mean_col]
        missing_mask = mean_values.isna()
        missing_rate = float(missing_mask.mean())
        missing_threshold = float(pass1_cfg["missing_rate"])
        missing_flag_mask = missing_mask & (missing_rate >= missing_threshold)

        spread = (overview[max_col] - overview[min_col]) <= pass1_cfg["flatline_eps"]
        flatline_mask = _flatline_mask(
            spread.fillna(False).to_list(),
            pass1_cfg["flatline_min_run"],
        )
        flatline_runs = 0
        flatline_max_run = 0
        current_run = 0
        for value in flatline_mask:
            if value:
                current_run += 1
            else:
                if current_run > 0:
                    flatline_runs += 1
                    flatline_max_run = max(flatline_max_run, current_run)
                current_run = 0
        if current_run > 0:
            flatline_runs += 1
            flatline_max_run = max(flatline_max_run, current_run)

        diffs = mean_values.diff()
        spike_z = _mad_z(diffs.fillna(0.0).to_list())
        spike_z_abs = np.abs(np.asarray(spike_z))
        spike_mask = spike_z_abs >= pass1_cfg["spike_mad_z"]
        spike_max = float(np.nanmax(spike_z_abs)) if spike_z_abs.size else 0.0

        flagged = [
            bool(missing or flatline or spike)
            for missing, flatline, spike in zip(
                missing_flag_mask, flatline_mask, spike_mask
            )
        ]

        per_signal[signal] = {
            "missing_rate": missing_rate,
            "missing_rate_flagged": bool(missing_rate >= missing_threshold),
            "flatline_run_count": int(flatline_runs),
            "flatline_max_run": int(flatline_max_run),
            "spike_mad_z_max": float(spike_max),
            "flagged_bucket_count": int(sum(flagged)),
            "flagged_buckets": [
                int(bucket)
                for bucket, is_flagged in zip(bucket_values, flagged)
                if is_flagged
            ],
        }
        flagged_by_signal[signal] = flagged

    union_flags = [False] * len(bucket_values)
    for flags in flagged_by_signal.values():
        union_flags = [u or f for u, f in zip(union_flags, flags)]

    windows: List[Dict[str, Any]] = []
    start_idx = None
    for idx, flagged in enumerate(union_flags):
        if flagged and start_idx is None:
            start_idx = idx
        elif not flagged and start_idx is not None:
            windows.append({"start_idx": start_idx, "end_idx": idx - 1})
            start_idx = None
    if start_idx is not None:
        windows.append({"start_idx": start_idx, "end_idx": len(union_flags) - 1})

    window_results: List[Dict[str, Any]] = []
    for window in windows:
        start_idx = window["start_idx"]
        end_idx = window["end_idx"]
        start_bucket = int(bucket_values[start_idx])
        end_bucket = int(bucket_values[end_idx])
        start_time = _format_time(time_values.iloc[start_idx])
        end_time = _format_time(time_values.iloc[end_idx])
        duration = end_bucket - start_bucket + 1
        signal_scores = []
        total_score = 0.0
        for signal in signals:
            flags = flagged_by_signal[signal][start_idx : end_idx + 1]
            flagged_count = int(sum(flags))
            spike_max = per_signal[signal]["spike_mad_z_max"]
            score = flagged_count + spike_max
            total_score += score
            signal_scores.append(
                {
                    "signal": signal,
                    "flagged_bucket_count": flagged_count,
                    "spike_mad_z_max": float(spike_max),
                    "score": float(score),
                }
            )
        signal_scores.sort(key=lambda item: (-item["score"], item["signal"]))
        window_results.append(
            {
                "start_bucket": start_bucket,
                "end_bucket": end_bucket,
                "start_time": start_time,
                "end_time": end_time,
                "duration_buckets": int(duration),
                "score": float(total_score),
                "signals": signal_scores,
            }
        )

    window_results.sort(key=lambda item: (-item["score"], item["start_bucket"]))

    return AutopsyResultPass1(
        measurement_id=measurement_id,
        overview_cfg=overview_cfg,
        pass1_cfg=pass1_cfg,
        key=key,
        created_at_unix=time.time(),
        per_signal=per_signal,
        timestamp_checks=timestamp_checks,
        windows=window_results,
        cache_hit=cache_hit,
    )
