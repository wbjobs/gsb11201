#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

BUILD_DIR=build
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

find src -name '*.java' -type f | sort > "$BUILD_DIR/sources.list"
javac -encoding UTF-8 --release 8 -d "$BUILD_DIR" @"$BUILD_DIR/sources.list"
java -ea -cp "$BUILD_DIR" com.gsb.cache.TestRunner
