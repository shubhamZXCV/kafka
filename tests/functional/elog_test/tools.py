"""Wrappers for elogctl, elog-producer and elog-consumer (LLD §15.2, R-18)."""

import json
import subprocess

from .cluster import Process


def elogctl(cluster, *args, check=True, timeout=30):
    result = subprocess.run(
        [str(cluster.bin / "elogctl"), "--coordinator", cluster.coordinator_address,
         *map(str, args)], capture_output=True, text=True, timeout=timeout)
    if check and result.returncode != 0:
        raise AssertionError(f"elogctl {' '.join(map(str, args))} exited "
                             f"{result.returncode}: {result.stderr.strip()}")
    return result


def create_topic(cluster, name, partitions, replication=1, min_isr=1, check=True):
    return elogctl(cluster, "create-topic", name, "-p", partitions, "-r", replication,
                   "--min-isr", min_isr, check=check)


def describe_topic(cluster, name):
    return json.loads(elogctl(cluster, "describe-topic", name).stdout)


def dump(cluster, topic, partition, start=0):
    """Committed records of one partition from `start`: dicts with offset, key, pid, seq."""
    out = elogctl(cluster, "dump", topic, partition, "--from", start, timeout=300).stdout
    return [json.loads(line) for line in out.splitlines()]


def log_verify(cluster, broker_id):
    """elogctl log-verify on one broker's data dir: (exit code, parsed report)."""
    result = subprocess.run([str(cluster.bin / "elogctl"), "log-verify",
                             str(cluster.root / f"broker-{broker_id}")],
                            capture_output=True, text=True, timeout=120)
    return result.returncode, json.loads(result.stdout) if result.stdout else None


def _flags(options):
    argv = []
    for flag, value in options.items():
        name = "--" + flag.replace("_", "-")
        argv += [name] if value is True else [name, str(value)]
    return argv


def start_producer(cluster, topic, out_dir, id="p1", **options):
    """Starts elog-producer (options: count, rate, keys, payload, seed)."""
    argv = [str(cluster.bin / "elog-producer"), "--coordinator", cluster.coordinator_address,
            "--topic", topic, "--id", id, "--out", str(out_dir), *_flags(options)]
    return cluster.track(Process(f"producer-{id}", argv, out_dir / f"producer-{id}.log").start())


def start_consumer(cluster, topic, out_dir, id="c1", **options):
    """Starts elog-consumer in manual mode (options: partitions, count, inflight, reset)."""
    argv = [str(cluster.bin / "elog-consumer"), "--coordinator", cluster.coordinator_address,
            "--topic", topic, "--manual", "--id", id, "--out", str(out_dir), *_flags(options)]
    return cluster.track(Process(f"consumer-{id}", argv, out_dir / f"consumer-{id}.log").start())
