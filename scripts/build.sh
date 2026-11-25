#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p build
stage=$(mktemp -d build/java.XXXXXX)
trap 'rm -rf -- "$stage"' EXIT
mkdir "$stage/classes"
find src/main/java -name '*.java' -type f | LC_ALL=C sort > "$stage/main.sources"
javac --release 21 -d "$stage/classes" @"$stage/main.sources"
jar --create --file "$stage/tidemq.jar" --main-class tidemq.ServerMain -C "$stage/classes" .
mv -- "$stage/tidemq.jar" build/tidemq.jar
