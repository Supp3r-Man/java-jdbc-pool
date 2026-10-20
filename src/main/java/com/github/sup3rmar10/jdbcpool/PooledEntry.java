package com.github.sup3rmar10.jdbcpool;

import java.sql.Connection;

/** Bookkeeping for one physical connection. Fields other than {@code broken} are guarded by the pool lock. */
final class PooledEntry {
    final Connection raw;
    final long createdAt;
    final long expiresAt;          // Long.MAX_VALUE when max lifetime is disabled
    final boolean defaultAutoCommit;
    final boolean defaultReadOnly;
    final int defaultIsolation;

    volatile boolean broken;       // set by the proxy after a connection-class SQLException or a failed reset
    boolean removed;               // already detached from the pool's accounting
    long lastReturned;             // for idle timeout
    long lastAccessed;             // for keepalive: last return or successful ping
    long borrowedAt;
    String borrowerName;
    Throwable borrowStack;
    boolean leakReported;

    PooledEntry(Connection raw, long createdAt, long expiresAt,
                boolean defaultAutoCommit, boolean defaultReadOnly, int defaultIsolation) {
        this.raw = raw;
        this.createdAt = createdAt;
        this.expiresAt = expiresAt;
        this.defaultAutoCommit = defaultAutoCommit;
        this.defaultReadOnly = defaultReadOnly;
        this.defaultIsolation = defaultIsolation;
        this.lastReturned = createdAt;
        this.lastAccessed = createdAt;
    }

    boolean isExpired(long now) {
        return now >= expiresAt;
    }
}
