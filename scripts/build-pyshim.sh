#!/usr/bin/env bash
# Builds the dev-only pyo3 shim (tests/pyshim) and installs it as tests/python/_sqlite_engine.so, the module the Python tests import.
# Usage: scripts/build-pyshim.sh [--release]
# Environment: CARGO_TARGET_DIR (default: tests/pyshim/target; keep it under $HOME, never /tmp).
set -euo pipefail

here="$(cd "$(dirname "$0")/.." && pwd)"
profile=debug
flags=()
# BRANCH: --release builds the optimized profile
if [ "${1:-}" = "--release" ]; then profile=release; flags=(--release); fi

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$here/tests/pyshim/target}"
(cd "$here/tests/pyshim" && cargo build ${flags[@]+"${flags[@]}"})

case "$(uname -s)" in
    Darwin) lib=lib_sqlite_engine.dylib ;;
    MINGW* | MSYS* | CYGWIN*) lib=_sqlite_engine.dll ;;
    *) lib=lib_sqlite_engine.so ;;
esac
dest=_sqlite_engine.so
# BRANCH: Windows extension modules are .pyd
case "$lib" in *.dll) dest=_sqlite_engine.pyd ;; esac
cp "$CARGO_TARGET_DIR/$profile/$lib" "$here/tests/python/$dest"
echo "installed tests/python/$dest"
