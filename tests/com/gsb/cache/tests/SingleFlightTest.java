package com.gsb.cache.tests;

import com.gsb.cache.DefaultCacheManager;
import com.gsb.cache.Stats;
import com.gsb.cache.Supplier;
import com.gsb.cache.SystemClock;
import com.gsb.cache.WriteMode;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/** Concurrent misses for the same key must collapse to one source load. */
final class SingleFlightTest {
    private SingleFlightTest() {}

    static void run() throws Exception {
        DefaultCacheManager cm = new DefaultCacheManager(new SystemClock());
        cm.register("r", 128, 60000L, WriteMode.WRITE_THROUGH);

        final int threads = 32;
        final AtomicInteger loads = new AtomicInteger();
        final Supplier slowLoader = new Supplier() {
            @Override
            public Object get() {
                loads.incrementAndGet();
                Check.sleep(200L);
                return "v";
            }
        };
        final CyclicBarrier start = new CyclicBarrier(threads);
        final Object[] results = new Object[threads];
        Thread[] workers = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            final int index = i;
            workers[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        start.await();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    results[index] = cm.get("r", "k", slowLoader);
                }
            });
        }
        for (Thread t : workers) {
            t.start();
        }
        for (Thread t : workers) {
            t.join(10000L);
            Check.isTrue(!t.isAlive(), "worker thread did not finish");
        }

        Check.eq(1L, loads.get(), "loader must run exactly once for concurrent misses");
        for (int i = 0; i < threads; i++) {
            Check.eq("v", results[i], "all waiters must receive the single loaded result");
        }
        Stats stats = cm.stats("r");
        Check.eq(1L, stats.getLoads(), "stats.loads");
        Check.eq(threads, stats.getMisses(), "stats.misses");
        cm.shutdown();
    }
}
