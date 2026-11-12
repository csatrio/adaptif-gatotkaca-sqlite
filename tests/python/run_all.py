#!/usr/bin/env python3
"""Small runner: each tests/python/test_*.py runs as its own process; exit 77 means SKIP.

Usage: python3 tests/python/run_all.py [name ...]

Exit status: 1 when any test failed; 77 (SKIP) when nothing failed, nothing passed and at least one test skipped (the whole
suite was skipped, which must not read as a pass); 0 only when at least one test passed and none failed.
Environment: ``GATOTKACA_TEST_TIMEOUT`` (seconds per test, default 1800; a timeout is a failure).
"""

import glob
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SKIP_EXIT = 77
TIMEOUT = int(os.environ.get("GATOTKACA_TEST_TIMEOUT", "1800"))


def main(argv):
    names = argv or sorted(os.path.basename(p)[:-3] for p in glob.glob(os.path.join(HERE, "test_*.py")))
    failed = skipped = passed = 0
    for n in names:
        n = n if n.startswith("test_") else "test_" + n
        try:
            r = subprocess.run([sys.executable, os.path.join(HERE, n + ".py")], timeout=TIMEOUT)
        except subprocess.TimeoutExpired:
            failed += 1
            print(f"FAIL {n} (timeout after {TIMEOUT}s)")
            continue
        if r.returncode == 0:
            passed += 1
            print(f"PASS {n}")
        elif r.returncode == SKIP_EXIT:
            skipped += 1
            print(f"SKIP {n}")
        else:
            failed += 1
            print(f"FAIL {n} (exit {r.returncode})")
    print(f"{passed} passed, {skipped} skipped, {failed} failed")
    # BRANCH: any failure is 1; an all-skipped (or empty) run is 77; otherwise at least one passed
    if failed:
        return 1
    if passed == 0:
        return SKIP_EXIT
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
