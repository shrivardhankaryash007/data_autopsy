from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


def sha256_file_signature(path: Path, head_bytes: int = 2_000_000) -> str:
    """
    Compute a stable-enough fingerprint for huge files without hashing the whole file.

    Strategy:
    - hash(file_size, mtime, first N bytes)
    Tradeoff:
    - Faster than full hash; extremely unlikely collisions for practical usage.
    """
    path = path.expanduser().resolve()
    stat = path.stat()

    h = hashlib.sha256()
    h.update(str(stat.st_size).encode())
    h.update(str(int(stat.st_mtime)).encode())

    with path.open("rb") as f:
        h.update(f.read(head_bytes))

    return h.hexdigest()


def stable_json_hash(obj: Dict[str, Any]) -> str:
    """Deterministic hash of a JSON-serializable dict (sorted keys)."""
    payload = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class MeasurementRef:
    """A stable handle to a measurement file."""
    id: str
    path: str
    label: Optional[str] = None


class MeasurementStore:
    """
    Persistent store for measurement metadata + derived artifacts.

    v0 scope:
    - register file
    - fingerprint file
    - cache lightweight metadata (no full-res load)
    - provide deterministic cache paths for future artifacts

    Future:
    - overview caches (downsampled min/mean/max)
    - autopsy results (json + report)
    """

    def __init__(self, root: str | Path = ".autopsy_cache"):
        self.root = Path(root)
        self.meta_dir = self.root / "meta"
        self.art_dir = self.root / "artifacts"
        self.meta_dir.mkdir(parents=True, exist_ok=True)
        self.art_dir.mkdir(parents=True, exist_ok=True)

    def add(self, path: str | Path, label: Optional[str] = None) -> MeasurementRef:
        path = Path(path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(path)

        fid = sha256_file_signature(path)
        mid = f"m_{fid[:12]}"
        meta_path = self.meta_dir / f"{mid}.json"

        if not meta_path.exists():
            meta = self._extract_metadata(path)
            meta.update(
                {
                    "measurement_id": mid,
                    "file_fingerprint": fid,
                    "path": str(path),
                    "label": label,
                    "created_at_unix": time.time(),
                }
            )
            meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

        # Update label (non-destructive)
        if label is not None:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if meta.get("label") != label:
                meta["label"] = label
                meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

        return MeasurementRef(id=mid, path=str(path), label=label)

    def meta(self, measurement_id: str) -> Dict[str, Any]:
        meta_path = self.meta_dir / f"{measurement_id}.json"
        if not meta_path.exists():
            raise KeyError(f"Unknown measurement_id: {measurement_id}")
        return json.loads(meta_path.read_text(encoding="utf-8"))

    def cache_path(self, measurement_id: str, kind: str, key: str, suffix: str) -> Path:
        """
        Return a deterministic cache path.

        Example:
          kind="overview", key="v1_1hz", suffix=".parquet"
        """
        d = self.art_dir / measurement_id / kind
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{key}{suffix}"

    def config_key(self, config: Dict[str, Any]) -> str:
        """Short stable key for a config dict."""
        return stable_json_hash(config)[:16]

    def _extract_metadata(self, path: Path) -> Dict[str, Any]:
        """
        Extract lightweight metadata without loading full data.

        v0 behavior:
        - MF4/MDF: attempt channel list via asammdf (best effort)
        - Other: minimal
        """
        suf = path.suffix.lower()
        if suf in {".mf4", ".mdf"}:
            try:
                from asammdf import MDF  # optional dependency
                mdf = MDF(str(path))
                channels = sorted(list(mdf.channels_db.keys()))
                start = None
                try:
                    if getattr(mdf.header, "start_time", None):
                        start = float(mdf.header.start_time.timestamp())
                except Exception:
                    start = None

                return {
                    "format": "mf4",
                    "channels_count": len(channels),
                    # guard: don't make meta files gigantic
                    "channels": channels[:2000],
                    "channels_truncated": len(channels) > 2000,
                    "start_time_unix": start,
                }
            except Exception as e:
                return {"format": "mf4", "metadata_error": repr(e)}

        return {"format": suf.lstrip("."), "note": "minimal metadata in v0"}

    def build_overview(
        self,
        measurement_id: str,
        signals: Optional[Iterable[str]] = None,
        hz: float = 1.0,
        agg: Iterable[str] = ("min", "mean", "max"),
        time_col: str = "timestamp",
    ) -> Dict[str, Any]:
        """
        Build or load an overview cache for a CSV measurement.

        If time_col exists, bucket by time into 1/hz-second buckets.
        Otherwise, bucket by row index assuming uniform sampling at 1 Hz and treat
        the row index as seconds before applying the same 1/hz-second bucketing.
        """
        if hz <= 0:
            raise ValueError("hz must be positive")

        meta = self.meta(measurement_id)
        path = Path(meta["path"])
        if path.suffix.lower() != ".csv":
            raise ValueError("overview cache only supports CSV inputs in v0")

        signals_list = list(signals) if signals is not None else None
        agg_list = list(agg)
        config = {
            "measurement_id": measurement_id,
            "signals": signals_list,
            "hz": hz,
            "agg": agg_list,
            "time_col": time_col,
        }
        key = self.config_key(config)
        out_path = self.cache_path(measurement_id, "overview", key, ".parquet")
        if out_path.exists():
            return {"path": out_path, "cache_hit": True, "config": config, "key": key}

        try:
            import pandas as pd
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise ImportError("pandas is required for overview caching") from exc

        df = pd.read_csv(path)
        if time_col in df.columns:
            time_values = df[time_col]
            if pd.api.types.is_numeric_dtype(time_values):
                seconds = pd.to_numeric(time_values, errors="coerce")
            else:
                timestamps = pd.to_datetime(time_values, errors="coerce", utc=True)
                seconds = timestamps.astype("int64") / 1_000_000_000
        else:
            seconds = pd.Series(df.index.to_numpy(), index=df.index, dtype="float64")

        bucket_index = (seconds * hz).floordiv(1).astype("Int64")
        bucket_seconds = bucket_index.astype("float64") / hz
        bucket_key = "bucket"
        df[bucket_key] = bucket_index

        if signals_list is None:
            signals_list = [
                col for col in df.columns if col not in {time_col, bucket_key}
            ]

        grouped = df.groupby(bucket_key)[signals_list].agg(agg_list)
        grouped.columns = [f"{col}_{stat}" for col, stat in grouped.columns]
        grouped = grouped.reset_index()

        grouped[time_col] = bucket_seconds.groupby(bucket_index).first().to_numpy()
        grouped = grouped[[time_col] + [col for col in grouped.columns if col != time_col]]

        if time_col in df.columns and not pd.api.types.is_numeric_dtype(time_values):
            grouped[time_col] = pd.to_datetime(
                grouped[time_col], 
                unit="s", 
                utc=True, 
                errors="coerce")
            grouped = grouped.dropna(subset=[time_col])


        grouped.to_parquet(out_path, index=False)
        return {"path": out_path, "cache_hit": False, "config": config, "key": key}

    def load_overview(self, measurement_id: str, config_or_key: Dict[str, Any] | str):
        """Load a cached overview DataFrame by config dict or key."""
        try:
            import pandas as pd
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise ImportError("pandas is required for overview caching") from exc

        if isinstance(config_or_key, dict):
            key = self.config_key(config_or_key)
        else:
            key = config_or_key

        path = self.cache_path(measurement_id, "overview", key, ".parquet")
        if not path.exists():
            raise FileNotFoundError(path)
        return pd.read_parquet(path)
