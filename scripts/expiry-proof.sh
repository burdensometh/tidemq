#!/usr/bin/env bash
# Rerunnable proof for lease-expiry scaling and stale-record fencing.
#   bash scripts/expiry-proof.sh
set -euo pipefail
cd "$(dirname "$0")/.."

bash scripts/build.sh

mkdir -p build/proof-classes
find proof -name '*.java' -type f | LC_ALL=C sort > build/proof.sources
javac --release 21 -cp build/tidemq.jar -d build/proof-classes @build/proof.sources

echo "== expiry check-cost scaling (white box) =="
java -ea -cp "build/proof-classes:build/tidemq.jar" tidemq.delivery.ExpiryScalingProof

echo
echo "== expiry fencing invariants over real TCP =="
java -ea -cp "build/proof-classes:build/tidemq.jar" proof.ExpiryInvariantProof

echo
echo "EXPIRY PROOF PASS"
