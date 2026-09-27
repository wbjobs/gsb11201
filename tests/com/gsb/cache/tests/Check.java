package com.gsb.cache.tests;

/** Tiny assertion helper so tests run with plain java, no JUnit. */
final class Check {
    private Check() {}

    static void isTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    static void eq(long expected, long actual, String message) {
        if (expected != actual) {
            throw new AssertionError(message + " (expected=" + expected
                    + ", actual=" + actual + ")");
        }
    }

    static void eq(Object expected, Object actual, String message) {
        boolean equal = expected == null ? actual == null : expected.equals(actual);
        if (!equal) {
            throw new AssertionError(message + " (expected=" + expected
                    + ", actual=" + actual + ")");
        }
    }

    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    static void await(BooleanCondition condition, long timeoutMillis,
            String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.get()) {
                return;
            }
            Thread.sleep(2L);
        }
        if (!condition.get()) {
            throw new AssertionError(message + " (timed out after "
                    + timeoutMillis + "ms)");
        }
    }

    interface BooleanCondition {
        boolean get();
    }
}
