"""Writes run.json for one experiment run (LLD §13.1): the experiment, git SHA, seed,
parameters, the effective process settings and the host it ran on.

    python3 run_meta.py RUN_DIR key=value ...
"""

import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SETTINGS = ["COORDINATOR_ARGS", "BROKER_ARGS", "PRODUCER_ARGS", "CONSUMER_ARGS", "TOPIC",
            "GROUP", "RATE", "COUNT"]


def git(*args):
    result = subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True)
    return result.stdout.strip()


def command_output(*argv):
    try:
        return subprocess.run(argv, capture_output=True, text=True, timeout=10).stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        return ""


def memory_bytes():
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemTotal:"):
            return int(line.split()[1]) * 1024
    return None


def main(run_dir, pairs):
    run_dir = Path(run_dir)
    params = dict(pair.split("=", 1) for pair in pairs)
    settings = {name: os.environ.get(name, "") for name in SETTINGS}
    settings.update({name: value for name, value in sorted(os.environ.items())
                     if name.startswith("CONSUMER_") and name.endswith("_ARGS")})
    broker_args = settings["BROKER_ARGS"]
    meta = {
        "experiment": run_dir.name.split("-", 1)[1],
        "started_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "git_sha": git("rev-parse", "HEAD"),
        "git_dirty": bool(git("status", "--porcelain", "--untracked-files=no")),
        "seed": int(params.pop("seed")),
        "params": params,
        "settings": settings,
        # The defaults unless an experiment overrides them (LLD §12.1, §12.2).
        "fsync_policy": "batch" if "log.flush.policy=batch" in broker_args else
                        "none" if "log.flush.policy=none" in broker_args else "interval",
        "fencing_enabled": "coordinator.fencing.enabled=false" not in settings["COORDINATOR_ARGS"],
        "topology": {"brokers": 3, "coordinator": 1},
        "host": {"platform": platform.platform(), "cpus": os.cpu_count(),
                 "memory_bytes": memory_bytes(),
                 "docker": command_output("docker", "version", "--format",
                                          "{{.Server.Version}}")},
    }
    (run_dir / "run.json").write_text(json.dumps(meta, indent=2) + "\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2:])
