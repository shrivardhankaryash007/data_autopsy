# Task: Step 2 - Overview Cache v0

- Date: 2026-01-21
- Status: completed
- Owner: codex

## Goal

Implement overview cache v0 for CSV measurements with deterministic caching, Parquet persistence, and tests.

## Prompt

Create docs/codex/tasks/2026-01-21_step2_overview_cache_v0.md from the template and fill it completely (including the exact prompt text). Implement Overview Cache v0:

Add to MeasurementStore:

build_overview(measurement_id, signals=None, hz=1.0, agg=("min","mean","max"), time_col="timestamp")

load_overview(measurement_id, config_or_key)

CSV behavior:

If time_col exists: bucket by time into 1/hz seconds.

Else: bucket by row index assuming uniform sampling (document this clearly).

Persist overview as Parquet and add dependencies (pandas, pyarrow) under an optional extra like [project.optional-dependencies].overview (or dev if simpler for now).

Cache key must use store.config_key(config) and store under artifacts/<measurement_id>/overview/.

Add unit tests with a small synthetic CSV that verify:

determinism (same config yields same file path)

caching (second run does not recompute; can check by mtime unchanged or a “cache hit” flag)

Run ruff check . and pytest -q.

Open a PR.

## Plan

1. Implement overview cache read/write on MeasurementStore with deterministic keys.
2. Add optional dependencies, update docs, and cover behavior with tests.
3. Run ruff and pytest, then record results.

## Steps

### Step 1

- Implement CSV overview build/load in the persistent MeasurementStore.
- Document overview cache behavior in README.

### Step 2

- Add optional dependencies and unit tests for determinism and caching behavior.
- Run ruff and pytest, then capture outcomes.

## Definition of Done

- Overview cache build/load is implemented with deterministic keys and Parquet output.
- Tests cover determinism and cache hit behavior for CSV inputs.
- Ruff and pytest are executed with recorded results.

## Notes

- Files changed: README.md; pyproject.toml; src/autopsy/store.py; tests/test_overview_cache.py; docs/codex/tasks/2026-01-21_step2_overview_cache_v0.md.
- Commands run:
  - `ruff check .` (pass)
  - `pytest -q` (pass; 7 passed, 1 skipped)
- Commit: 036b20d
- PR: N/A (no remote configured; recorded locally via make_pr)
