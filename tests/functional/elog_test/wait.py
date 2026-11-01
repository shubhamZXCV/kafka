"""wait_until: poll a condition instead of sleeping for a fixed time (LLD §15.2)."""

import time


def wait_until(predicate, timeout, what="condition", interval=0.05):
    """Return predicate()'s first truthy value; fail the test if none within `timeout` s."""
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out after {timeout} s waiting for {what}")
        time.sleep(interval)
