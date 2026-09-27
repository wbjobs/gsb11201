package com.gsb.cache.tests;

import com.gsb.cache.CacheWriter;
import com.gsb.cache.DefaultCacheManager;
import com.gsb.cache.Stats;
import com.gsb.cache.WriteMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/** Ten writes to one key while flushing is frozen must flush exactly once. */
final class WriteBehindCoalesceTest {
    private WriteBehindCoalesceTest() {}

    static void run() throws Exception {
        DefaultCacheManager cm = new DefaultCacheManager(new MutableClock(0L));
        cm.register("r", 16, 60000L, WriteMode.WRITE_BEHIND);

        final List<String> flushed = new ArrayList<String>();
        final AtomicInteger writeCalls = new AtomicInteger();
        cm.setWriter("r", new CacheWriter() {
            @Override
            public void write(String key, Object value) {
                synchronized (flushed) {
                    flushed.add(key + "=" + value);
                }
                writeCalls.incrementAndGet();
            }
        });

        // Freeze the worker before it can drain anything.
        final CountDownLatch gate = new CountDownLatch(1);
        cm.setPreFlushHook("r", new Runnable() {
            @Override
            public void run() {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        });

        for (int i = 1; i <= 10; i++) {
            cm.put("r", "k", "v" + i);
        }
        Check.eq(1, cm.stats("r").getPendingWrites(),
                "ten puts to one key must coalesce into one pending slot");

        gate.countDown();

        Check.await(new Check.BooleanCondition() {
            @Override
            public boolean get() {
                return cm.stats("r").getFlushes() >= 1L;
            }
        }, 5000L, "coalesced flush never happened");

        // Give a wrongly-behaving implementation time to flush extra times.
        Check.sleep(200L);
        Stats stats = cm.stats("r");
        Check.eq(1L, stats.getFlushes(), "ten consecutive writes must flush exactly once");
        Check.eq(1, writeCalls.get(), "writer must be invoked exactly once");
        synchronized (flushed) {
            Check.eq(1, flushed.size(), "one flushed record");
            Check.eq("k=v10", flushed.get(0), "flushed value must be the newest one");
        }
        Check.eq(0, stats.getPendingWrites(), "queue must be empty after flush");
        cm.shutdown();
    }
}
