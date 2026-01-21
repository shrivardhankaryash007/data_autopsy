from __future__ import annotations

from datetime import datetime
from pathlib import Path

TEMPLATE = Path("docs/codex/tasks/TEMPLATE.md")
TASKS_DIR = Path("docs/codex/tasks")
LEDGER = Path("docs/codex/ledger.md")

def slugify(s: str) -> str:
    return "".join(c.lower() if c.isalnum() else "_" for c in s).strip("_")

def main(title: str) -> Path:
    TASKS_DIR.mkdir(parents=True, exist_ok=True)
    date = datetime.now().strftime("%Y-%m-%d")
    slug = slugify(title)[:50]
    task_path = TASKS_DIR / f"{date}_{slug}.md"

    if task_path.exists():
        return task_path

    tpl = TEMPLATE.read_text(encoding="utf-8")
    task_path.write_text(tpl.replace("<short name>", title), encoding="utf-8")

    # append to ledger index (simple, deterministic)
    if LEDGER.exists():
        ledger = LEDGER.read_text(encoding="utf-8").rstrip() + "\n"
    else:
        ledger = "# Codex Prompt Ledger\n\n## Index\n"
    ledger += f"- [{task_path.name}]({task_path.as_posix()})\n"
    LEDGER.write_text(ledger, encoding="utf-8")

    return task_path

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python tools/new_task.py \"Task title\"")
    print(main(sys.argv[1]))
