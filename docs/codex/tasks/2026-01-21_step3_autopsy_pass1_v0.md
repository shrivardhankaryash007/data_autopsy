# Task: Autopsy Pass-1 v0

- Date: 2026-01-21
- Status: completed
- Owner: codex

## Goal

Implement Autopsy Pass-1 v0 using cached overview data, add tests, and document the
outcomes in a Codex task record.

## Prompt

Create docs/codex/tasks/2026-01-21_step3_autopsy_pass1_v0.md from the template and fill it completely (including the exact prompt text). Implement Autopsy Pass-1 v0 using ONLY the cached overview (no full-res data access):

Add run_autopsy_pass1() and load_autopsy_pass1() to MeasurementStore (or a new autopsy/pass1.py module used by the store).

Inputs:

overview_cfg: hz, agg, signals selection

pass1_cfg: thresholds (missing_rate, flatline_eps, flatline_min_run, spike_mad_z, top_k_windows, top_n_signals)

Compute per-signal QC metrics from overview:

missing_rate

flatline runs using (max-min) <= eps across consecutive buckets

spike score using robust MAD z-score on diff of mean

timestamp checks (monotonic, gaps)

Build candidate anomaly windows by grouping consecutive “flagged” buckets (union across signals), score each window, and rank.

Output:

Cache JSON under artifacts/<measurement_id>/autopsy_pass1/ using config_key.

Also write executive_summary.md (Top-K windows with top-N signals each; keep it compact).

Add dataclasses or typed dict schema for the result (AutopsyResultPass1).

Add unit tests using a synthetic CSV that produces a known window (e.g., signal_a flatlines for 10 buckets; signal_b spikes at t=50s). Tests must verify determinism + caching (second run is cache hit).

Update the notebook with a small demo cell that registers toy CSV, builds overview, runs pass1, and prints the summary.

Run ruff check . and pytest -q. Open a PR.

## Plan

1. Implement pass-1 analysis and caching in the autopsy store.
2. Add tests and notebook demo to exercise pass-1.
3. Update documentation and record results in the Codex task log.

## Steps

### Step 1

- Implement AutopsyResultPass1 and pass-1 analysis over cached overview data.
- Add MeasurementStore APIs for running and loading pass-1.
- Write executive summary output alongside JSON cache.

### Step 2

- Add deterministic, cached unit tests using a synthetic CSV dataset.
- Update the notebook with a pass-1 demo cell.

### Step 3

- Update documentation and finalize the task log with commands, files, PR, and commit details.

## Definition of Done

- Pass-1 results are cached from overview data only and can be reloaded deterministically.
- Unit tests cover the synthetic window detection and cache-hit behavior.
- Notebook demo runs pass-1 and prints the executive summary.

## Files Changed

- README.md
- docs/codex/tasks/2026-01-21_step3_autopsy_pass1_v0.md
- notebooks/Autopsy_Lab.ipynb
- src/autopsy/pass1.py
- src/autopsy/store.py
- tests/test_autopsy_pass1.py

## Commands Run

- ruff check .
  - Result: success
- pytest -q
  - Result: success (7 passed, 2 skipped)

## PR

- PR: N/A (no remote configured; recorded locally via make_pr)
- Commit: 5a4e258

## Notes

- Pass-1 analysis operates only on cached overview parquet files to avoid full-res loading.
