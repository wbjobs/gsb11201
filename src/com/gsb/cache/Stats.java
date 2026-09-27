package com.gsb.cache;

/** Immutable per-region statistics snapshot. */
public final class Stats {
    private final long l1Hits;
    private final long l2Hits;
    private final long misses;
    private final long loads;
    private final long puts;
    private final long invalidations;
    private final long l1Evictions;
    private final long flushes;
    private final int l1Size;
    private final int l2Size;
    private final int pendingWrites;

    public Stats(long l1Hits, long l2Hits, long misses, long loads, long puts,
            long invalidations, long l1Evictions, long flushes,
            int l1Size, int l2Size, int pendingWrites) {
        this.l1Hits = l1Hits;
        this.l2Hits = l2Hits;
        this.misses = misses;
        this.loads = loads;
        this.puts = puts;
        this.invalidations = invalidations;
        this.l1Evictions = l1Evictions;
        this.flushes = flushes;
        this.l1Size = l1Size;
        this.l2Size = l2Size;
        this.pendingWrites = pendingWrites;
    }

    public long getL1Hits() { return l1Hits; }
    public long getL2Hits() { return l2Hits; }
    public long getMisses() { return misses; }
    /** Number of times the backing loader was actually invoked. */
    public long getLoads() { return loads; }
    public long getPuts() { return puts; }
    public long getInvalidations() { return invalidations; }
    public long getL1Evictions() { return l1Evictions; }
    /** Number of writes delivered to the backing CacheWriter. */
    public long getFlushes() { return flushes; }
    public int getL1Size() { return l1Size; }
    public int getL2Size() { return l2Size; }
    /** Number of keys currently waiting in the WRITE_BEHIND queue. */
    public int getPendingWrites() { return pendingWrites; }
}
