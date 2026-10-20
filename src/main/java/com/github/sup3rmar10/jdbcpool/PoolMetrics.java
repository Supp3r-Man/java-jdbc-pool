package com.github.sup3rmar10.jdbcpool;

/**
 * A point-in-time snapshot. The gauges (active, idle, waiting, total) are read together under the pool lock;
 * the counters are read just after, so under heavy load the two groups can be a moment apart.
 *
 * @param active   connections currently lent out
 * @param idle     connections sitting in the pool
 * @param waiting  threads blocked in getConnection
 * @param total    connections the pool is accountable for (idle + lent + being created, validated or pinged)
 * @param timeouts borrows that gave up with a PoolTimeoutException
 * @param created  physical connections opened successfully
 * @param evicted  physical connections closed by a pool decision (idle, lifetime, failed validation, broken)
 */
public record PoolMetrics(int active, int idle, int waiting, int total,
                          long timeouts, long created, long evicted,
                          long leaksDetected, long validationFailures, long createFailures) {
}
