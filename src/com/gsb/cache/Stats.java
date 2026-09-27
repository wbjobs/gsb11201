package com.gsb.cache;

/**
 * 某个 region 在某一时刻的统计快照（不可变）。
 */
public final class Stats {
    private final long l1Hits;
    private final long l2Hits;
    private final long misses;
    private final long loads;
    private final long l1Evictions;
    private final long invalidations;
    private final long puts;
    private final long writeThroughWrites;
    private final long writeBehindFlushes;
    private final long writeBehindCoalesced;
    private final long queueFullBlocks;
    private final long pendingWrites;

    Stats(long l1Hits, long l2Hits, long misses, long loads, long l1Evictions,
          long invalidations, long puts, long writeThroughWrites,
          long writeBehindFlushes, long writeBehindCoalesced,
          long queueFullBlocks, long pendingWrites) {
        this.l1Hits = l1Hits;
        this.l2Hits = l2Hits;
        this.misses = misses;
        this.loads = loads;
        this.l1Evictions = l1Evictions;
        this.invalidations = invalidations;
        this.puts = puts;
        this.writeThroughWrites = writeThroughWrites;
        this.writeBehindFlushes = writeBehindFlushes;
        this.writeBehindCoalesced = writeBehindCoalesced;
        this.queueFullBlocks = queueFullBlocks;
        this.pendingWrites = pendingWrites;
    }

    public long getL1Hits() {
        return l1Hits;
    }

    public long getL2Hits() {
        return l2Hits;
    }

    public long getMisses() {
        return misses;
    }

    /** 实际回源加载次数（并发未命中合并后）。 */
    public long getLoads() {
        return loads;
    }

    public long getL1Evictions() {
        return l1Evictions;
    }

    public long getInvalidations() {
        return invalidations;
    }

    public long getPuts() {
        return puts;
    }

    public long getWriteThroughWrites() {
        return writeThroughWrites;
    }

    /** WRITE_BEHIND 下实际刷写到底层存储的次数。 */
    public long getWriteBehindFlushes() {
        return writeBehindFlushes;
    }

    /** WRITE_BEHIND 下因同 key 合并而省掉的排队次数。 */
    public long getWriteBehindCoalesced() {
        return writeBehindCoalesced;
    }

    /** 因队列满而阻塞等待的次数。 */
    public long getQueueFullBlocks() {
        return queueFullBlocks;
    }

    /** 快照时 WRITE_BEHIND 队列中待刷写的条目数。 */
    public long getPendingWrites() {
        return pendingWrites;
    }

    @Override
    public String toString() {
        return "Stats{l1Hits=" + l1Hits
                + ", l2Hits=" + l2Hits
                + ", misses=" + misses
                + ", loads=" + loads
                + ", l1Evictions=" + l1Evictions
                + ", invalidations=" + invalidations
                + ", puts=" + puts
                + ", writeThroughWrites=" + writeThroughWrites
                + ", writeBehindFlushes=" + writeBehindFlushes
                + ", writeBehindCoalesced=" + writeBehindCoalesced
                + ", queueFullBlocks=" + queueFullBlocks
                + ", pendingWrites=" + pendingWrites
                + '}';
    }
}
