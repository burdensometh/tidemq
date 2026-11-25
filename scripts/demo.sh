#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
bash scripts/compile-tools.sh
work=$(mktemp -d)
pid=
cleanup() {
    if [[ -n "$pid" ]]; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi
    rm -rf -- "$work"
}
trap cleanup EXIT
cat > "$work/broker.properties" <<EOF
listen.host=127.0.0.1
listen.port=0
data.directory=$work/data
topics=orders:3
segment.bytes=4096
message.max.bytes=2048
delivery.lease.ms=750
delivery.max.attempts=2
retention.min.age.ms=0
retention.interval.ms=60000
EOF
java -jar build/tidemq.jar "$work/broker.properties" > "$work/broker.log" 2>&1 &
pid=$!
port=
for (( attempt=0; attempt<100; attempt++ )); do
    port=$(sed -n 's/^TIDEMQ_READY .* port=\([0-9]*\) .*/\1/p' "$work/broker.log")
    if [[ -n "$port" ]]; then break; fi
    if ! kill -0 "$pid" 2>/dev/null; then break; fi
    sleep 0.05
done
cat "$work/broker.log"
[[ -n "$port" ]] || { echo "Broker did not become ready" >&2; exit 1; }
java -cp build/tidemq.jar tidemq.ClientMain 127.0.0.1 "$port" ping
java -cp build/tidemq.jar:build/test-classes tidemq.BrokerDemo 127.0.0.1 "$port"
java -cp build/tidemq.jar tidemq.ClientMain 127.0.0.1 "$port" partition orders 0
