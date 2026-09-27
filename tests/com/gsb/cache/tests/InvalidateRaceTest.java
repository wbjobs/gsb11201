package com.gsb.cache.tests;

import com.gsb.cache.DefaultCacheManager;
import com.gsb.cache.Supplier;
import com.gsb.cache.SystemClock;
import com.gsb.cache.WriteMode;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A load that is already in flight when invalidate() runs must not be allowed
 * to publish its (potentially stale) result back into L1/L2 afterwards.
 */
final class InvalidateRaceTest {
    private InvalidateRaceTest() {}

    static void run() throws Exception {
        final DefaultCacheManager cm = new DefaultCacheManager(new SystemClock());
        cm.register("r", 16, 60000L, WriteMode.WRITE_THROUGH);

        final AtomicInteger loads = new AtomicInteger();
        final CountDownLatch loadStarted = new CountDownLatch(1);
        final CountDownLatch finishLoad = new CountDownLatch(1);
        final Supplier loader = new Supplier() {
            @Override
            public Object get() {
                int n = loads.incrementAndGet();
                if (n == 1) {
                    loadStarted.countDown();
                    try {
                        finishLoad.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    return "old-value";
                }
                return "new-value";
            }
        };

        final AtomicReference<Object> inFlightResult = new AtomicReference<Object>();
        Thread inFlight = new Thread(new Runnable() {
            @Override
            public void run() {
                inFlightResult.set(cm.get("r", "k", loader));
            }
        });
        inFlight.start();
        loadStarted.await();

        cm.invalidate("r", "k");
        finishLoad.countDown();
        inFlight.join(10000L);
        Check.isTrue(!inFlight.isAlive(), "in-flight get never returned");

        // The caller that started before invalidation receives the value it
        // asked for; the point is that this value must not be cached.
        Check.eq("old-value", inFlightResult.get(), "in-flight caller result");

        Object afterInvalidate = cm.get("r", "k", loader);
        Check.eq("new-value", afterInvalidate,
                "after invalidation the stale in-flight value must never be readable");
        Check.eq(2, loads.get(), "a fresh source load must occur after invalidation");

        Supplier mustNotLoad = new Supplier() {
            @Override
            public Object get() {
                throw new AssertionError("fresh value must already be cached");
            }
        };
        Check.eq("new-value", cm.get("r", "k", mustNotLoad),
                "new value must remain cached in L1/L2");
        Check.eq(2, loads.get(), "cached new value must not trigger another load");
        cm.shutdown();
    }
}
