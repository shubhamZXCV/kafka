"""Fixtures for the functional tests (LLD §15.2)."""

import pytest

from elog_test.cluster import Cluster, build_dir as _build_dir


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: long or repeated scenario (not in the default run)")


@pytest.fixture(scope="session")
def build_dir():
    path = _build_dir()
    if not (path / "bin" / "elog-broker").exists():
        pytest.exit(f"no binaries in {path}/bin: build first, or set ELOG_BUILD_DIR", 2)
    return path


@pytest.fixture
def make_cluster(build_dir, tmp_path):
    """make_cluster(brokers=N, ...) starts a cluster; every one is shut down and its logs
    checked when the test ends, pass or fail."""
    clusters = []

    def make(brokers=1, **settings):
        cluster = Cluster(build_dir, tmp_path / f"cluster-{len(clusters)}", brokers, **settings)
        clusters.append(cluster)
        return cluster.start()

    yield make
    for cluster in clusters:
        cluster.shutdown()


@pytest.fixture
def out_dir(tmp_path):
    path = tmp_path / "out"
    path.mkdir()
    return path
