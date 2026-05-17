#!/usr/bin/env bash
# scripts/version.sh — print the resolved batchq version string.
#
# Output rules (first match wins):
#   1. HEAD is on an exact vX.Y.Z tag        → vX.Y.Z
#   2. A vX.Y.Z tag exists in history        → vX.Y.{Z+1}-dev-<short-sha>
#   3. Git is available but no v* tags yet   → v0.0.0-dev-<short-sha>
#   4. Not in a git repo (e.g. tarball)      → dev
#
# Only v*-prefixed tags are considered; legacy batchq-X.Y.Z tags are
# ignored. Consumed by the Makefile and by the GH Actions build.

set -e

if t=$(git describe --tags --exact-match --match 'v*' 2>/dev/null); then
	echo "$t"
	exit 0
fi

if latest=$(git describe --tags --abbrev=0 --match 'v*' 2>/dev/null); then
	base=${latest#v}
	IFS=. read -r major minor patch <<< "$base"
	if [[ "$patch" =~ ^[0-9]+$ ]]; then
		next=$((patch + 1))
		sha=$(git rev-parse --short HEAD)
		echo "v${major}.${minor}.${next}-dev-${sha}"
		exit 0
	fi
fi

if sha=$(git rev-parse --short HEAD 2>/dev/null); then
	echo "v0.0.0-dev-${sha}"
	exit 0
fi

echo "dev"
