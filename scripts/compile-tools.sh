#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
test -f build/tidemq.jar || bash scripts/build.sh
mkdir -p build/test-classes
find src/test/java -name '*.java' -type f | LC_ALL=C sort > build/test.sources
javac --release 21 -cp build/tidemq.jar -d build/test-classes @build/test.sources
