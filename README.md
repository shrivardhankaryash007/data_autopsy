# data_autopsy
This is a project that helps with data autopsy and root cause analysis.

## Overview cache v0

The `MeasurementStore` can build cached overview files for CSV measurements. The overview
groups data into 1/hz-second buckets, aggregating selected signal columns (min/mean/max by
default) and saving the result as Parquet under the store's `artifacts/<measurement_id>/overview/`
directory. When a CSV does not have a `timestamp` column (or the configured `time_col`), the
overview logic assumes uniform sampling at 1 Hz and uses the row index as seconds before
bucketing. This behavior is intentionally explicit so downstream consumers know the time
base is synthetic when timestamps are missing.

## Autopsy pass-1 v0

The `MeasurementStore` can run a lightweight pass-1 autopsy using only cached overview
files. Pass-1 computes per-signal quality checks (missing rate, flatline runs, spike
scores) plus timestamp integrity checks, then groups flagged buckets into ranked anomaly
windows. Results are cached as JSON and a compact executive summary under
`artifacts/<measurement_id>/autopsy_pass1/` using the pass-1 config key.
