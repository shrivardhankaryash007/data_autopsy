# Agent Operating Rules (Codex)

- Make one focused change per task.
- Update docs and add/adjust tests for any behavior change.
- Run `pytest -q` and `ruff check .` (or explain why you couldn't).
- Record the exact prompt and outcomes in `docs/codex/tasks/`.
- Do NOT add raw measurement data files to the repo.
- Prefer coarse-to-fine processing; avoid loading full-res MF4 into RAM.
- For every task you do, create or update a file in `docs/codex/tasks/`:
  - include the EXACT user prompt you were given (verbatim)
  - list files changed
  - list commands run + results
  - link the PR and commit SHA
