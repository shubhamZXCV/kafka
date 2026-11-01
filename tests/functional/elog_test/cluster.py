"""A coordinator and N brokers, each a real process on a free port (LLD §15.2)."""

import os
import re
import signal
import socket
import subprocess
from pathlib import Path

from .wait import wait_until

# A sanitizer report anywhere in a log fails the test (NFR-CONC-01).
SANITIZER_REPORT = re.compile(
    r"ERROR: AddressSanitizer|ERROR: LeakSanitizer|WARNING: ThreadSanitizer|runtime error:")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def port_open(port):
    with socket.socket() as s:
        s.settimeout(0.2)
        return s.connect_ex(("127.0.0.1", port)) == 0


class Process:
    """One binary with its log file. stop() is graceful, kill() is a crash."""

    def __init__(self, name, argv, log_path):
        self.name, self.argv, self.log_path = name, argv, Path(log_path)
        self.popen = None

    def start(self):
        log = open(self.log_path, "ab")
        self.popen = subprocess.Popen(self.argv, stdout=log, stderr=subprocess.STDOUT)
        log.close()
        return self

    def running(self):
        return self.popen is not None and self.popen.poll() is None

    def stop(self, timeout=10):
        """SIGTERM, wait; SIGKILL if it does not exit. Returns the exit code."""
        if not self.running():
            return None if self.popen is None else self.popen.returncode
        self.resume()
        self.popen.send_signal(signal.SIGTERM)
        try:
            return self.popen.wait(timeout)
        except subprocess.TimeoutExpired:
            self.popen.kill()
            self.popen.wait()
            raise AssertionError(f"{self.name} did not exit within {timeout} s of SIGTERM")

    def kill(self):
        if self.running():
            self.popen.kill()
            self.popen.wait()

    def pause(self):
        if self.running():
            self.popen.send_signal(signal.SIGSTOP)

    def resume(self):
        if self.running():
            self.popen.send_signal(signal.SIGCONT)

    def restart(self):
        self.stop()
        return self.start()

    def wait(self, timeout):
        return self.popen.wait(timeout)

    def log(self):
        return self.log_path.read_text(errors="replace") if self.log_path.exists() else ""


class Cluster:
    """Coordinator plus `brokers` brokers under `root`, with short test timeouts."""

    def __init__(self, build_dir, root, brokers=1, broker_settings=None,
                 coordinator_settings=None):
        self.bin = Path(build_dir) / "bin"
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.processes = []
        self.coordinator_port = free_port()
        self.coordinator_address = f"127.0.0.1:{self.coordinator_port}"
        settings = {"listen.port": self.coordinator_port, "db.path": self.root / "coordinator.db",
                    "broker.session.timeout.ms": 3000, **(coordinator_settings or {})}
        self.coordinator = self._process("coordinator", "elog-coordinator", settings)
        self.brokers = {}
        for broker_id in range(1, brokers + 1):
            port = free_port()
            settings = {"broker.id": broker_id, "listen.port": port,
                        "advertised.host": "127.0.0.1",
                        "coordinator.address": self.coordinator_address,
                        "data.dir": self.root / f"broker-{broker_id}",
                        "broker.heartbeat.interval.ms": 500, **(broker_settings or {})}
            self.brokers[broker_id] = self._process(f"broker-{broker_id}", "elog-broker",
                                                    settings)
            self.brokers[broker_id].port = port

    def _process(self, name, binary, settings):
        argv = [str(self.bin / binary)]
        for key, value in settings.items():
            argv += ["--set", f"{key}={value}"]
        process = Process(name, argv, self.root / f"{name}.log")
        self.processes.append(process)
        return process

    def start(self, timeout=20):
        self.coordinator.start()
        wait_until(lambda: port_open(self.coordinator_port), timeout, "coordinator listening")
        for broker in self.brokers.values():
            broker.start()
        self.wait_healthy(len(self.brokers), timeout)
        return self

    def wait_healthy(self, brokers, timeout=20):
        def healthy():
            result = subprocess.run(
                [str(self.bin / "elogctl"), "--coordinator", self.coordinator_address, "health",
                 "--brokers", str(brokers)], capture_output=True)
            return result.returncode == 0
        wait_until(healthy, timeout, f"{brokers} live brokers")

    def track(self, process):
        """Adds a tool process so teardown also stops and checks it."""
        self.processes.append(process)
        return process

    def shutdown(self):
        """Stops everything (tools first, coordinator last) and checks every log."""
        problems = []
        for process in reversed(self.processes):
            try:
                process.stop()
            except AssertionError as e:
                problems.append(str(e))
        for process in self.processes:
            if SANITIZER_REPORT.search(process.log()):
                problems.append(f"sanitizer report in {process.log_path}")
        if problems:
            raise AssertionError("; ".join(problems))


def build_dir():
    """ELOG_BUILD_DIR selects the binaries (LLD §15.2); the dev build by default."""
    root = Path(__file__).resolve().parents[3]
    return Path(os.environ.get("ELOG_BUILD_DIR", root / "build" / "dev")).resolve()
