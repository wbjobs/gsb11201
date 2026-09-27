package com.gsb.cache.tests;

import com.gsb.cache.DefaultCacheManager;
import com.gsb.cache.Stats;
import com.gsb.cache.Supplier;
import com.gsb.cache.SystemClock;
import com.gsb.cache.WriteMode;

/** L1 eviction must follow strict least-recently-used order. */
final class LruEvictionTest {
    private LruEvictionTest() {}

    private static final Supplier FAILING_LOADER = new Supplier() {
        @Override
        public Object get() {
            throw new AssertionError("every key was pre-populated; loader must never run");
        }
    };

    static void run() {
        DefaultCacheManager cm = new DefaultCacheManager(new SystemClock());
        cm.register("r", 3, 3600000L, WriteMode.WRITE_THROUGH);

        cm.put("r", "a", "A");
        cm.put("r", "b", "B");
        cm.put("r", "c", "C");

        // Touch a: access order becomes b, c, a.
        Check.eq("A", cm.get("r", "a", FAILING_LOADER), "a L1 hit");

        // Insert d: the least-recently-used entry is b, so b must be evicted.
        cm.put("r", "d", "D");
        Stats stats = cm.stats("r");
        Check.eq(1L, stats.getL1Evictions(), "b must be the first eviction");

        // c, a, d must still live in L1; ordering c, a, d.
        Check.eq("C", cm.get("r", "c", FAILING_LOADER), "c still in L1");
        Check.eq("A", cm.get("r", "a", FAILING_LOADER), "a still in L1");
        Check.eq("D", cm.get("r", "d", FAILING_LOADER), "d still in L1");

        // b is gone from L1 but still in L2.
        Check.eq("B", cm.get("r", "b", FAILING_LOADER), "b served from L2 after LRU eviction");
        stats = cm.stats("r");
        Check.eq(1L, stats.getL2Hits(), "b L2 hit");
        // Promoting b back must itself evict the current LRU entry (c).
        Check.eq(2L, stats.getL1Evictions(), "c evicted when b is promoted");

        // Touch a and d: access order b, a, d.
        Check.eq("A", cm.get("r", "a", FAILING_LOADER), "a L1 hit");
        Check.eq("D", cm.get("r", "d", FAILING_LOADER), "d L1 hit");

        // Insert e: LRU is b, so b is evicted again.
        cm.put("r", "e", "E");
        stats = cm.stats("r");
        Check.eq(3L, stats.getL1Evictions(), "b must be the third eviction");
        Check.eq("B", cm.get("r", "b", FAILING_LOADER), "b served from L2 again");
        stats = cm.stats("r");
        Check.eq(2L, stats.getL2Hits(), "b L2 hit again");
        Check.eq(4L, stats.getL1Evictions(), "a evicted when b is promoted");

        // Remaining L1 contents a... after promotion the order is d, e, b.
        Check.eq("D", cm.get("r", "d", FAILING_LOADER), "d L1 hit");
        Check.eq("E", cm.get("r", "e", FAILING_LOADER), "e L1 hit");
        Check.eq("B", cm.get("r", "b", FAILING_LOADER), "b L1 hit");

        stats = cm.stats("r");
        Check.eq(0L, stats.getLoads(), "no source load allowed in LRU test");
        Check.eq(9L, stats.getL1Hits(), "exact L1 hit count");
        Check.eq(3L, stats.getL1Size(), "L1 stays bounded at capacity");
        cm.shutdown();
    }
}
