#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
bash scripts/compile-tools.sh
java -ea -cp build/tidemq.jar:build/test-classes tidemq.BrokerTest "$@"
