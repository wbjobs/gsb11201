package com.gsb.cache.tests;

import com.gsb.cache.DefaultCacheManager;
import com.gsb.cache.Supplier;
import com.gsb.cache.WriteMode;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * L2 entry loaded (or put) at time t with ttl D is live while now < t + D and
 * expired at the exact boundary millisecond t + D.
 */
final class TtlBoundaryTest {
    private TtlBoundaryTest() {}

    static void run() {
        MutableClock clock = new MutableClock(1_000_000L);
        DefaultCacheManager cm = new DefaultCacheManager(clock);
        // L1 disabled so this test observes L2 TTL behavior directly.
        cm.register("r", 0, 1000L, WriteMode.WRITE_THROUGH);

        final AtomicInteger loads = new AtomicInteger();
        Supplier loader = new Supplier() {
            @Override
            public Object get() {
                int n = loads.incrementAndGet();
                return "v" + n;
            }
        };

        long t0 = clock.currentTimeMillis();
        Check.eq("v1", cm.get("r", "k", loader), "first get loads v1");
        Check.eq(1, loads.get(), "first load");
        long expireAt = t0 + 1000L;

        clock.setTime(expireAt - 1L);
        Check.eq("v1", cm.get("r", "k", loader),
                "one millisecond before the boundary the value must still be live");
        Check.eq(1, loads.get(), "no reload before boundary");

        clock.setTime(expireAt);
        Check.eq("v2", cm.get("r", "k", loader),
                "at the exact boundary millisecond the value must be expired");
        Check.eq(2, loads.get(), "reload at boundary");

        clock.setTime(expireAt + 999L);
        Check.eq("v2", cm.get("r", "k", loader), "second value still live");
        Check.eq(2, loads.get(), "no reload while live");

        clock.setTime(expireAt + 1000L);
        Check.eq("v3", cm.get("r", "k", loader), "second value expires at its own boundary");
        Check.eq(3, loads.get(), "third reload");

        // Same boundary semantics for put().
        Supplier fail = new Supplier() {
            @Override
            public Object get() {
                throw new AssertionError("put value should be served while live");
            }
        };
        long putTime = clock.currentTimeMillis();
        cm.put("r", "p", "P");
        clock.setTime(putTime + 999L);
        Check.eq("P", cm.get("r", "p", fail), "put value live at boundary-1");
        Supplier pLoader = new Supplier() {
            @Override
            public Object get() {
                return "P-loaded";
            }
        };
        clock.setTime(putTime + 1000L);
        Check.eq("P-loaded", cm.get("r", "p", pLoader),
                "put value expired at the exact boundary millisecond");

        cm.shutdown();
    }
}
