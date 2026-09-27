#!/usr/bin/env bash
# Compile src/ and tests/ with javac (Java 8 source/target level) and run the
# plain-Java test suite. No Maven, Gradle, JUnit or other third-party libraries.
set -euo pipefail
cd "$(dirname "$0")"

BUILD_DIR=build
SRC_OUT="$BUILD_DIR/classes"
TEST_OUT="$BUILD_DIR/test-classes"

echo "==> Cleaning $BUILD_DIR/"
rm -rf "$BUILD_DIR"
mkdir -p "$SRC_OUT" "$TEST_OUT"

echo "==> Compiling src/ with javac (-source 8 -target 8)"
find src -name '*.java' -print > "$BUILD_DIR/src.list"
javac -source 8 -target 8 -encoding UTF-8 -Xlint:-options \
      -d "$SRC_OUT" @"$BUILD_DIR/src.list"

echo "==> Compiling tests/"
find tests -name '*.java' -print > "$BUILD_DIR/test.list"
javac -source 8 -target 8 -encoding UTF-8 -Xlint:-options \
      -cp "$SRC_OUT" -d "$TEST_OUT" @"$BUILD_DIR/test.list"

echo "==> Running tests"
java -ea -cp "$SRC_OUT:$TEST_OUT" com.gsb.cache.tests.TestRunner
