#!/usr/bin/env bash
# Standalone check of adaptif-gatotkaca-sqlite: the SQLite engine.
# It copies this repo alone (it has no sibling dependency) to a fresh scratch directory OUTSIDE the ~/rust-dev tree (so no relative path can reach
# the main repo `adaptif-gatotkaca` or any other checkout), then checks and builds there:
#   1. manifest scan: path dependencies only to the declared closure, no git/registry dependency, no root [workspace];
#   2. stray-reference scan of the copied sources, manifests, scripts, tests and docs (`adaptif-gatotkaca/`,
#      `../adaptif-gatotkaca` not followed by `-`, `/Users/`), with an explicit allow-list (STRAY_ALLOW below);
#   3. `cargo metadata` guard: it must succeed, and no `adaptif-gatotkaca-*` package may come from outside the closure
#      (the only crates.io ones allowed are named in EXTERNAL_OK);
#   4. `cargo fmt --check`, `cargo build --all-targets`, `cargo clippy --all-targets -- -D warnings`, `cargo test`, and
#      the pyo3 shim build (scripts/build-pyshim.sh) and tests/python/run_all.py (exit 0 = passed, 77 = all skipped and reported as such,
#      anything else fails): tests whose optional tool (tortoise, wrk) is missing skip.
# Usage: [STANDALONE_CHECK_ONLY=1] scripts/standalone-check.sh [--keep]
#   STANDALONE_CHECK_ONLY=1  stop after the scans and `cargo check --all-targets`
#   STANDALONE_TMP=dir       parent of the scratch directory (default $HOME/standalone-check-scratch; must not be below a directory that holds the
#                            main repo; keep it on a large volume). The scratch path is printed; it is removed on success and
#                            kept on failure (or with --keep).
#   CARGO_TARGET_DIR         reuse a target directory (default: inside the scratch directory)
set -euo pipefail

REPO=adaptif-gatotkaca-sqlite
CLOSURE=()   # sibling repositories this repo declares a dependency on
EXTERNAL_OK="adaptif-gatotkaca-utils"   # crates.io packages named adaptif-gatotkaca-* that may appear in the graph
# Stray-reference allow-list: "file|extended regex" pairs (file relative to the repo root, regex matched against the hit line).
STRAY_ALLOW=('README.md|\.\./adaptif-gatotkaca')

here="$(cd "$(dirname "$0")/.." && pwd)"
parent="$(dirname "$here")"
keep=0
for a in "$@"; do [ "$a" = "--keep" ] && keep=1; done

tmp_parent="${STANDALONE_TMP:-$HOME/standalone-check-scratch}"
mkdir -p "$tmp_parent"
scratch="$(mktemp -d "${tmp_parent%/}/standalone.XXXXXX")"
scratch="$(cd "$scratch" && pwd -P)"
echo "standalone scratch: $scratch"
ok=0
cleanup() {
    # BRANCH: removed only after a full pass and without --keep; otherwise kept for inspection
    if [ "$ok" = 1 ] && [ "$keep" = 0 ]; then rm -rf "$scratch"; else echo "scratch kept: $scratch" >&2; fi
}
trap cleanup EXIT
fail() { echo "standalone check FAILED: $*" >&2; exit 1; }

# The scratch directory must not sit below any directory that holds the main repo.
d="$scratch"
while [ "$d" != "/" ]; do
    [ ! -e "$d/adaptif-gatotkaca" ] || fail "scratch is below $d, which holds the main repo (set STANDALONE_TMP elsewhere)"
    d="$(dirname "$d")"
done

# Copy this repo and its closure (tar: present everywhere, unlike rsync).
for d in "$REPO" ${CLOSURE[@]+"${CLOSURE[@]}"}; do
    [ -d "$parent/$d" ] || fail "missing sibling: $parent/$d"
    mkdir -p "$scratch/$d"
    (cd "$parent/$d" && tar --exclude=target --exclude=.git --exclude=.venv --exclude=__pycache__ -cf - .) | (cd "$scratch/$d" && tar -xf -)
done
# the scratch parent holds exactly the closure
want="$(printf '%s\n' "$REPO" ${CLOSURE[@]+"${CLOSURE[@]}"} | sort | tr '\n' ' ')"
have="$(ls "$scratch" | sort | tr '\n' ' ')"
[ "$have" = "$want" ] || fail "unexpected content in $scratch: $have (wanted $want)"

# ---- 1. manifests: path dependencies only into the closure, no git/registry dependency, no root [workspace]
scan_manifests() {
    local f v rest first c found resolved bad=0
    if grep -nE '^\[workspace' Cargo.toml; then echo "root Cargo.toml has a [workspace] table" >&2; bad=1; fi
    while IFS= read -r f; do
        if grep -nE '^[[:space:]]*(git|registry|index)[[:space:]]*=|\{[^}]*[[:space:]](git|registry)[[:space:]]*=' "$f"; then
            echo "$f: git or registry dependency" >&2; bad=1
        fi
        while IFS= read -r v; do
            [ -n "$v" ] || continue
            # BRANCH: a path that resolves inside this repo (the pyo3 shim depends on the repo root) is not a sibling
            resolved="$(cd "$(dirname "$f")" 2>/dev/null && cd "$v" 2>/dev/null && pwd -P || true)"
            case "$resolved/" in "$(pwd -P)"/*) continue ;; esac
            case "$v" in
                ..) continue ;;   # a fuzz crate pointing at its parent crate
                ../*) ;;
                *) continue ;;    # inside this repo
            esac
            rest="$v"
            while [ "${rest#../}" != "$rest" ]; do rest="${rest#../}"; done
            first="${rest%%/*}"
            found=0
            for c in ${CLOSURE[@]+"${CLOSURE[@]}"}; do [ "$first" = "$c" ] && found=1; done
            if [ "$found" = 0 ]; then echo "$f: path outside the closure: $v" >&2; bad=1; fi
        done < <(grep -oE 'path[[:space:]]*=[[:space:]]*"[^"]*"' "$f" | sed -E 's/^[^"]*"//; s/"$//')
    done < <(find . -name Cargo.toml -not -path '*/target/*')
    [ "$bad" = 0 ] || fail "manifest scan"
}

# ---- 2. stray references to the main repo or to this machine
STRAY_RE='adaptif-gatotkaca/|\.\./adaptif-gatotkaca([^-]|$)|/Users/'
scan_stray() { # $1 = directory name inside the scratch parent; the allow-list applies to $REPO only
    local dir="$1" hits line file text pair afile apat allowed bad=0
    hits="$(cd "$scratch/$dir" && grep -rInE "$STRAY_RE" . --exclude-dir=target --exclude-dir=.git --exclude-dir=__pycache__ \
        --exclude=Cargo.lock --exclude=standalone-check.sh || true)"
    [ -n "$hits" ] || return 0
    while IFS= read -r line; do
        file="${line%%:*}"; file="${file#./}"
        text="${line#*:}"; text="${text#*:}"
        allowed=0
        if [ "$dir" = "$REPO" ]; then
            for pair in ${STRAY_ALLOW[@]+"${STRAY_ALLOW[@]}"}; do
                afile="${pair%%|*}"; apat="${pair#*|}"
                if [ "$file" = "$afile" ] && [[ "$text" =~ $apat ]]; then allowed=1; fi
            done
        fi
        if [ "$allowed" = 0 ]; then echo "stray reference in $dir: $line" >&2; bad=1; fi
    done <<< "$hits"
    [ "$bad" = 0 ] || fail "stray reference to the main repo or to this machine"
}

# ---- 3. dependency graph: metadata must succeed; no adaptif-gatotkaca-* package from outside the closure
check_metadata() {
    local meta="$scratch/metadata.json"
    cargo metadata --format-version 1 > "$meta" || fail "cargo metadata failed"
    python3 -I - "$meta" "$scratch" "$REPO" "$EXTERNAL_OK" ${CLOSURE[@]+"${CLOSURE[@]}"} <<'PY' || fail "dependency graph"
import json, os, sys
meta_path, scratch, repo, external = sys.argv[1:5]
closure = [repo] + sys.argv[5:]
external = set(external.split())
scratch = os.path.realpath(scratch)
roots = {name: os.path.join(scratch, name) for name in closure}
bad = []
for p in json.load(open(meta_path))["packages"]:
    name, src = p["name"], p["source"]
    mdir = os.path.realpath(os.path.dirname(p["manifest_path"]))
    ours = name == "adaptif-gatotkaca" or name.startswith("adaptif-gatotkaca-")
    if src is None:  # a path package: it must live inside one repo of the closure
        if not any(mdir == r or mdir.startswith(r + os.sep) for r in roots.values()):
            bad.append(f"path package {name} outside the closure: {mdir}")
        elif ours and name not in closure:
            bad.append(f"unexpected path package {name} at {mdir}")
    elif ours and name not in external:
        bad.append(f"{name} {p['version']} comes from {src} but is not an allowed crates.io package")
    elif ours and not (src.startswith("registry+https://github.com/rust-lang/crates.io-index") or src.startswith("sparse+https://index.crates.io")):
        bad.append(f"{name} comes from a non-crates.io source: {src}")
for b in bad:
    print("  " + b, file=sys.stderr)
sys.exit(1 if bad else 0)
PY
}

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$scratch/target}"
cd "$scratch/$REPO"
scan_manifests
for d in "$REPO" ${CLOSURE[@]+"${CLOSURE[@]}"}; do scan_stray "$d"; done
check_metadata

if [ -n "${STANDALONE_CHECK_ONLY:-}" ]; then
    cargo check --all-targets
else
    cargo fmt --all --check
    cargo build --all-targets
    cargo clippy --all-targets -- -D warnings
    cargo test
    # build the dev-only pyo3 shim (own target directory), then run the Python tests against it
    CARGO_TARGET_DIR="$scratch/target-pyshim" scripts/build-pyshim.sh
    # Python tests are run by their runner. Exit 0 = at least one passed and none failed; 77 = every test skipped (reported, not a
    # pass); anything else is a failure.
    py_rc=0
    python3 tests/python/run_all.py || py_rc=$?
    case "$py_rc" in
        0) echo "python tests: passed" ;;
        77) echo "python tests: ALL SKIPPED (exit 77): nothing ran" ;;
        *) fail "python tests (exit $py_rc)" ;;
    esac
fi
ok=1
echo "standalone check passed: $REPO"
