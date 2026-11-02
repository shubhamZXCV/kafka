"""Consumer capacity per producer rate, from an experiments/calibrate.sh run directory:
the group's consume rate from 10 s after the consumers start (joined, queues full) until
they stop, while the preloaded backlog lasts. Writes calibration.json.

    python3 analysis/calibration.py runs/<id>-calibration
"""

import json
import sys
from pathlib import Path

from elog_analysis.loaders import load_metrics, load_phases
from elog_analysis.timing import consume_rate

SETTLE_MS = 10_000


def main(run_dir):
    run_dir = Path(run_dir)
    rate, phases = consume_rate(load_metrics(run_dir)), load_phases(run_dir)
    results = {}
    for phase, start in phases.items():
        if phase.startswith("consume-"):
            producer = int(phase.split("-")[1])
            window = rate[(rate.index >= start + SETTLE_MS) &
                          (rate.index < phases[f"stop-{producer}"] - 1000)]
            results[producer] = {"group_rate": window.mean(), "per_consumer": window.mean() / 4,
                                 "cv": window.std() / window.mean()}
            print(f"producer {producer:>5}/s: consumers {window.mean():7.0f}/s "
                  f"({window.mean() / 4:.0f} each, CV {window.std() / window.mean():.2f})")
    (run_dir / "calibration.json").write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
