package com.github.sup3rmar10.jdbcpool;

import static com.github.sup3rmar10.jdbcpool.Support.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Everything here is driven by a manual clock and a manual scheduler: no sleeping, no real time. */
class HousekeepingTest {
    private final FakeDb db = new FakeDb();
    private final ManualClock clock = new ManualClock();
    private final ManualScheduler scheduler = new ManualScheduler();
    private final List<LeakReport> reports = Collections.synchronizedList(new ArrayList<>());
    private JdbcPool pool;

    /** Quiet baseline: no idle timeout, no lifetime, no keepalive, no leak detection. */
    private PoolConfig.Builder base() {
        return cfg(db, clock, scheduler).idleTimeoutMillis(0).maxLifetimeMillis(0);
    }

    private JdbcPool pool(PoolConfig.Builder b) {
        pool = new JdbcPool(b.build());
        return pool;
    }

    private void passTime(long ms) {
        clock.advance(ms);
        scheduler.tick();
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.shutdown(0);
        }
    }

    // ---------------------------------------------------------------- idle eviction

    @Test
    void idleConnectionsAboveMinAreEvictedOnceTheyHaveBeenIdleLongEnough() throws Exception {
        pool(base().minSize(1).maxSize(3).idleTimeoutMillis(1000));
        Connection a = pool.getConnection();
        Connection b = pool.getConnection();
        Connection c = pool.getConnection();
        a.close();
        b.close();
        c.close();
        passTime(999);
        assertEquals(3, pool.metrics().total());
        passTime(1);
        PoolMetrics m = pool.metrics();
        assertEquals(1, m.total());
        assertEquals(1, m.idle());
        assertEquals(2, m.evicted());
        assertEquals(1, db.live.get());
    }

    @Test
    void idleEvictionNeverGoesBelowMin() throws Exception {
        pool(base().minSize(2).maxSize(4).idleTimeoutMillis(1000));
        passTime(3_600_000);
        assertEquals(2, pool.metrics().total());
        assertEquals(0, pool.metrics().evicted());
    }

    @Test
    void aRecentlyReturnedConnectionOutlivesAnOlderIdleOne() throws Exception {
        pool(base().minSize(0).maxSize(2).idleTimeoutMillis(1000));
        Connection a = pool.getConnection();
        Connection b = pool.getConnection();
        a.close();
        b.close();
        clock.advance(600);
        pool.getConnection().close(); // lends and returns the most recently returned one (fake-1)
        passTime(400);
        assertEquals(1, pool.metrics().total());
        assertTrue(db.conn(0).closed);
        assertFalse(db.conn(1).closed);
    }

    @Test
    void idleTimeoutZeroDisablesIdleEviction() throws Exception {
        pool(base().minSize(0).maxSize(2));
        Connection a = pool.getConnection();
        Connection b = pool.getConnection();
        a.close();
        b.close();
        passTime(1_000_000_000L);
        assertEquals(2, pool.metrics().total());
    }

    // ---------------------------------------------------------------- max lifetime and jitter

    @Test
    void anIdleConnectionIsClosedAtMaxLifetimeAndTheMinIsRestored() throws Exception {
        pool(base().minSize(1).maxSize(2).maxLifetimeMillis(10_000).maxLifetimeJitterMillis(0));
        passTime(9_999);
        assertEquals(1, pool.metrics().created());
        passTime(1);
        assertTrue(db.conn(0).closed);
        PoolMetrics m = pool.metrics();
        assertEquals(2, m.created());
        assertEquals(1, m.evicted());
        assertEquals(1, m.idle());
    }

    @Test
    void jitterShortensTheLifetimeAndIsDrawnWithTheDocumentedBound() throws Exception {
        List<Long> bounds = Collections.synchronizedList(new ArrayList<>());
        pool(base().minSize(1).maxSize(1).maxLifetimeMillis(10_000).maxLifetimeJitterMillis(1_000)
                .jitterSource(bound -> {
                    bounds.add(bound);
                    return bound - 1; // the largest possible jitter, i.e. 1000 ms
                }));
        assertEquals(1_001L, bounds.get(0));
        passTime(8_999);
        assertFalse(db.conn(0).closed);
        passTime(1);
        assertTrue(db.conn(0).closed);
    }

    @Test
    void zeroJitterFromTheSourceMeansTheFullLifetime() throws Exception {
        pool(base().minSize(1).maxSize(1).maxLifetimeMillis(10_000).maxLifetimeJitterMillis(1_000)
                .jitterSource(bound -> 0));
        passTime(9_999);
        assertFalse(db.conn(0).closed);
        passTime(1);
        assertTrue(db.conn(0).closed);
    }

    @Test
    void jitterIsDrawnSeparatelyForEachConnection() throws Exception {
        AtomicInteger n = new AtomicInteger();
        pool(base().minSize(3).maxSize(3).maxLifetimeMillis(10_000).maxLifetimeJitterMillis(1_000)
                .jitterSource(bound -> Math.min(bound - 1, n.getAndIncrement() * 500L))); // 0, 500, 1000, then 1000
        passTime(9_000);
        assertEquals(1, pool.metrics().evicted()); // only the connection with 1000 ms jitter
        passTime(500);
        assertEquals(2, pool.metrics().evicted());
        passTime(500);
        assertEquals(3, pool.metrics().evicted());
    }

    @Test
    void noJitterIsDrawnWhenJitterIsZero() throws Exception {
        pool(base().minSize(1).maxSize(1).maxLifetimeMillis(10_000).maxLifetimeJitterMillis(0)
                .jitterSource(bound -> {
                    throw new AssertionError("jitter source must not be consulted");
                }));
        assertEquals(1, pool.metrics().created());
    }

    @Test
    void aLentConnectionPastItsLifetimeIsNotClosedUnderTheCallerButIsClosedOnReturn() throws Exception {
        pool(base().minSize(0).maxSize(1).maxLifetimeMillis(10_000).maxLifetimeJitterMillis(0));
        Connection c = pool.getConnection();
        passTime(20_000);
        assertFalse(db.conn(0).closed);
        assertEquals(1, pool.metrics().active());
        c.close();
        assertTrue(db.conn(0).closed);
        PoolMetrics m = pool.metrics();
        assertEquals(0, m.total());
        assertEquals(1, m.evicted());
    }

    @Test
    void lifetimeZeroMeansConnectionsLiveForever() throws Exception {
        pool(base().minSize(1).maxSize(1));
        passTime(Long.MAX_VALUE / 4);
        assertFalse(db.conn(0).closed);
    }

    @Test
    void housekeepingRestoresMinAfterABrokenReturn() throws Exception {
        pool(base().minSize(2).maxSize(2));
        Connection c = pool.getConnection();
        c.setAutoCommit(false);
        db.conn(1).failures.put("rollback", new SQLException("reset failed"));
        db.conn(0).failures.put("rollback", new SQLException("reset failed"));
        c.close();
        assertEquals(1, pool.metrics().total());
        passTime(1);
        assertEquals(2, pool.metrics().total());
        assertEquals(2, pool.metrics().idle());
    }

    // ---------------------------------------------------------------- keepalive

    @Test
    void keepaliveOffMeansNoPings() throws Exception {
        pool(base().minSize(1).maxSize(1));
        passTime(10_000_000);
        assertEquals(0, db.conn(0).validCalls.get());
    }

    @Test
    void keepalivePingsIdleConnectionsAtTheConfiguredInterval() throws Exception {
        pool(base().minSize(1).maxSize(1).keepaliveMillis(1000).validationTimeoutSeconds(3));
        passTime(999);
        assertEquals(0, db.conn(0).validCalls.get());
        passTime(1);
        assertEquals(1, db.conn(0).validCalls.get());
        assertEquals(3, db.conn(0).lastValidTimeout);
        assertEquals(1, pool.metrics().idle());
        passTime(500);
        assertEquals(1, db.conn(0).validCalls.get()); // the ping counted as activity
        passTime(500);
        assertEquals(2, db.conn(0).validCalls.get());
    }

    @Test
    void aDeadIdleConnectionFoundByKeepaliveIsEvictedAndReplaced() throws Exception {
        pool(base().minSize(1).maxSize(1).keepaliveMillis(1000));
        db.conn(0).valid = false;
        passTime(1000);
        assertTrue(db.conn(0).closed);
        PoolMetrics m = pool.metrics();
        assertEquals(1, m.validationFailures());
        assertEquals(1, m.evicted());
        assertEquals(2, m.created());
        assertEquals(1, m.total());
    }

    @Test
    void keepaliveDoesNotTouchLentConnections() throws Exception {
        pool(base().minSize(0).maxSize(1).keepaliveMillis(1000));
        Connection c = pool.getConnection();
        passTime(10_000);
        assertEquals(0, db.conn(0).validCalls.get());
        c.close();
    }

    // ---------------------------------------------------------------- leak detection

    private Connection borrowHere() throws SQLException {
        return pool.getConnection();
    }

    private PoolConfig.Builder leaky() {
        return base().minSize(0).maxSize(2).leakThresholdMillis(5_000).leakListener(reports::add);
    }

    @Test
    void aLeakIsReportedWithTheStackOfTheBorrowOnceTheThresholdPasses() throws Exception {
        pool(leaky());
        Connection c = borrowHere();
        passTime(4_999);
        assertTrue(reports.isEmpty());
        passTime(1);
        assertEquals(1, reports.size());
        LeakReport r = reports.get(0);
        assertEquals(5_000, r.heldMillis());
        assertEquals(Thread.currentThread().getName(), r.threadName());
        assertTrue(r.describe().contains("borrowHere"), r.describe());
        assertEquals(1, pool.metrics().leaksDetected());
        c.close();
    }

    @Test
    void aConnectionReturnedBeforeTheThresholdIsNeverReported() throws Exception {
        pool(leaky());
        Connection c = borrowHere();
        passTime(4_000);
        c.close();
        passTime(100_000);
        assertTrue(reports.isEmpty());
        assertEquals(0, pool.metrics().leaksDetected());
    }

    @Test
    void aLeakIsReportedOnlyOnce() throws Exception {
        pool(leaky());
        Connection c = borrowHere();
        passTime(5_000);
        passTime(5_000);
        passTime(5_000);
        assertEquals(1, reports.size());
        c.close();
    }

    @Test
    void leakDetectionIsOffWhenTheThresholdIsZero() throws Exception {
        pool(base().minSize(0).maxSize(1).leakListener(reports::add));
        Connection c = pool.getConnection();
        passTime(1_000_000);
        assertTrue(reports.isEmpty());
        c.close();
    }

    @Test
    void theSameConnectionBorrowedAgainCanBeReportedAgain() throws Exception {
        pool(leaky().maxSize(1));
        Connection first = borrowHere();
        passTime(5_000);
        first.close();
        Connection second = borrowHere();
        passTime(5_000);
        assertEquals(2, reports.size());
        second.close();
    }

    @Test
    void aFailingLeakListenerDoesNotStopTheRestOfHousekeeping() throws Exception {
        pool(base().minSize(0).maxSize(2).idleTimeoutMillis(1000).leakThresholdMillis(5_000)
                .leakListener(r -> {
                    throw new IllegalStateException("listener bug");
                }));
        Connection leaked = pool.getConnection();
        Connection other = pool.getConnection();
        other.close();
        passTime(5_000);
        PoolMetrics m = pool.metrics();
        assertEquals(1, m.leaksDetected());
        assertEquals(1, m.total()); // the idle one was still evicted in the same pass
        leaked.close();
    }

    // ---------------------------------------------------------------- scheduling

    @Test
    void housekeepingIsScheduledOnceWithTheConfiguredPeriodAndCancelledOnShutdown() {
        pool(base().housekeepingPeriodMillis(250));
        assertEquals(250, scheduler.period);
        assertEquals(1, scheduler.scheduledCount);
        assertFalse(scheduler.cancelled);
        pool.shutdown(0);
        assertTrue(scheduler.cancelled);
    }
}
