"""summary.json for one run (LLD §16.1 step 6): the experiment's checks and values."""

import json
from pathlib import Path


def write_summary(run_dir, checks, values):
    """`checks` is {name: (passed, detail)}; `values` is {name: number or text}. Prints
    one line per check and returns whether all passed."""
    summary = {
        "passed": all(passed for passed, _ in checks.values()),
        "checks": {name: {"passed": bool(passed), "detail": detail}
                   for name, (passed, detail) in checks.items()},
        "values": values,
    }
    Path(run_dir, "summary.json").write_text(json.dumps(summary, indent=2, default=float) + "\n")
    for name, (passed, detail) in checks.items():
        print(f"{'PASS' if passed else 'FAIL'}  {name}: {detail}")
    return summary["passed"]
