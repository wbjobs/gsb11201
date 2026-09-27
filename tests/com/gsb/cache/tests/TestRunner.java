package com.gsb.cache.tests;

/** Plain-Java test runner (no JUnit). Exit code 0 only if every test passes. */
public final class TestRunner {
    private TestRunner() {}

    public static void main(String[] args) {
        TestCase[] tests = new TestCase[] {
            new TestCase("concurrent-miss-single-flight") {
                @Override void run() throws Exception { SingleFlightTest.run(); }
            },
            new TestCase("write-behind-coalesces-10-writes-to-1-flush") {
                @Override void run() throws Exception { WriteBehindCoalesceTest.run(); }
            },
            new TestCase("write-behind-queue-full-backpressure-no-loss") {
                @Override void run() throws Exception { WriteBehindQueueFullTest.run(); }
            },
            new TestCase("l1-lru-eviction-order") {
                @Override void run() { LruEvictionTest.run(); }
            },
            new TestCase("invalidate-vs-inflight-load-race") {
                @Override void run() throws Exception { InvalidateRaceTest.run(); }
            },
            new TestCase("l2-ttl-exact-boundary") {
                @Override void run() { TtlBoundaryTest.run(); }
            },
            new TestCase("write-through-synchronous-flush") {
                @Override void run() { WriteThroughTest.run(); }
            },
        };

        int failures = 0;
        for (int i = 0; i < tests.length; i++) {
            TestCase tc = tests[i];
            String prefix = "[" + (i + 1) + "/" + tests.length + "] " + tc.name;
            try {
                tc.run();
                System.out.println("PASS " + prefix);
            } catch (Throwable t) {
                failures++;
                System.out.println("FAIL " + prefix + " -> " + t);
                t.printStackTrace(System.out);
            }
        }
        System.out.println();
        if (failures == 0) {
            System.out.println("ALL " + tests.length + " TESTS PASSED");
        } else {
            System.out.println(failures + "/" + tests.length + " TESTS FAILED");
            System.exit(1);
        }
    }

    private abstract static class TestCase {
        final String name;
        TestCase(String name) { this.name = name; }
        abstract void run() throws Exception;
    }
}
