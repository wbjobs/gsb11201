package com.gsb.cache.tests;

import com.gsb.cache.CacheWriter;
import com.gsb.cache.DefaultCacheManager;
import com.gsb.cache.Stats;
import com.gsb.cache.Supplier;
import com.gsb.cache.SystemClock;
import com.gsb.cache.WriteMode;
import java.util.ArrayList;
import java.util.List;

/** WRITE_THROUGH hands the value to the writer synchronously inside put(). */
final class WriteThroughTest {
    private WriteThroughTest() {}

    static void run() {
        DefaultCacheManager cm = new DefaultCacheManager(new SystemClock());
        cm.register("r", 16, 60000L, WriteMode.WRITE_THROUGH);

        final List<String> flushed = new ArrayList<String>();
        cm.setWriter("r", new CacheWriter() {
            @Override
            public void write(String key, Object value) {
                synchronized (flushed) {
                    flushed.add(key + "=" + value);
                }
            }
        });

        cm.put("r", "k", "v");
        synchronized (flushed) {
            Check.eq(1, flushed.size(), "write-through must flush synchronously before put returns");
            Check.eq("k=v", flushed.get(0), "flushed value");
        }

        Supplier fail = new Supplier() {
            @Override
            public Object get() {
                throw new AssertionError("value must be served from L1");
            }
        };
        Check.eq("v", cm.get("r", "k", fail), "L1 hit after write-through put");
        Stats stats = cm.stats("r");
        Check.eq(1L, stats.getFlushes(), "one synchronous flush");
        Check.eq(1L, stats.getL1Hits(), "one L1 hit");
        Check.eq(0, stats.getPendingWrites(), "no write-behind queue in through mode");
        cm.shutdown();
    }
}
