#!/usr/bin/env bash
#
# End-to-end smoke runner for batchq (untracked dev helper; see CLAUDE.md).
# Builds the binary, then in an isolated throwaway BATCHQ_HOME: submits a
# trivial job, runs it with the simple runner (which drains and exits once
# idle), shows it, and cleans up. Exits non-zero on any failure.
#
# Usage: ./test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$ROOT/bin/batchq.$(go env GOOS)_$(go env GOARCH)"

export BATCHQ_HOME
BATCHQ_HOME="$(mktemp -d)"
trap 'rm -rf "$BATCHQ_HOME"' EXIT

echo "==> building"
make -C "$ROOT" >/dev/null

echo "==> BATCHQ_HOME=$BATCHQ_HOME"

echo "==> submit"
JOBID="$("$BIN" submit -- echo "hello from batchq")"
echo "submitted: $JOBID"

echo "==> run (simple runner drains then exits)"
"$BIN" run

echo "==> show"
"$BIN" show "$JOBID"

echo "==> cleanup"
"$BIN" cleanup "$JOBID"

echo "==> OK"
