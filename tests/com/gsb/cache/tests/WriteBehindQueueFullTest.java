package com.gsb.cache.tests;

import com.gsb.cache.CacheWriter;
import com.gsb.cache.DefaultCacheManager;
import com.gsb.cache.WriteMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Queue-full policy: back-pressure. A put that would exceed the bounded
 * WRITE_BEHIND queue blocks until the worker frees space; nothing is dropped.
 */
final class WriteBehindQueueFullTest {
    private WriteBehindQueueFullTest() {}

    static void run() throws Exception {
        final int capacity = 2;
        DefaultCacheManager cm = new DefaultCacheManager(new MutableClock(0L), capacity);
        cm.register("r", 16, 60000L, WriteMode.WRITE_BEHIND);

        final List<String> flushed = new ArrayList<String>();
        cm.setWriter("r", new CacheWriter() {
            @Override
            public void write(String key, Object value) {
                synchronized (flushed) {
                    flushed.add(key + "=" + value);
                }
            }
        });

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

        cm.put("r", "k1", "v1");
        cm.put("r", "k2", "v2");
        Check.eq(capacity, cm.stats("r").getPendingWrites(), "queue fills to capacity");

        final CountDownLatch blockedPutFinished = new CountDownLatch(1);
        Thread blocked = new Thread(new Runnable() {
            @Override
            public void run() {
                cm.put("r", "k3", "v3");
                blockedPutFinished.countDown();
            }
        });
        blocked.start();

        Check.sleep(200L);
        Check.isTrue(blocked.isAlive(),
                "put must block under back-pressure when the queue is full");
        Check.eq(capacity, cm.stats("r").getPendingWrites(),
                "blocked put must not have overflowed the queue");

        gate.countDown();

        Check.await(new Check.BooleanCondition() {
            @Override
            public boolean get() {
                return blockedPutFinished.getCount() == 0L;
            }
        }, 5000L, "blocked put never completed after queue drained");
        blocked.join(5000L);

        Check.await(new Check.BooleanCondition() {
            @Override
            public boolean get() {
                return cm.stats("r").getFlushes() == 3L;
            }
        }, 5000L, "not all three queued writes were flushed");
        Check.eq(0, cm.stats("r").getPendingWrites(), "queue empty after draining");
        synchronized (flushed) {
            Check.eq(3, flushed.size(), "all writes delivered, none lost");
            Check.eq("k1=v1", flushed.get(0), "first write");
            Check.eq("k2=v2", flushed.get(1), "second write");
            Check.eq("k3=v3", flushed.get(2), "third (previously blocked) write");
        }
        cm.shutdown();
    }
}
