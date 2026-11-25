#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
bash scripts/compile-tools.sh
java -cp build/tidemq.jar:build/test-classes tidemq.HotPathBench "$@"
