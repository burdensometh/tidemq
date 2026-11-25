#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
test "$#" -eq 1 || { echo "Usage: bash scripts/samples.sh NEW_DIRECTORY" >&2; exit 2; }
bash scripts/compile-tools.sh
java -cp build/tidemq.jar:build/test-classes tidemq.Fixtures "$1"
