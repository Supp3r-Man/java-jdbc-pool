package com.github.sup3rmar10.jdbcpool;

import static com.github.sup3rmar10.jdbcpool.Support.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ShutdownTest {
    private final FakeDb db = new FakeDb();
    private final ManualClock clock = new ManualClock();
    private final ManualScheduler scheduler = new ManualScheduler();
    private JdbcPool pool;

    private JdbcPool pool(PoolConfig.Builder b) {
        pool = new JdbcPool(b.build());
        return pool;
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.shutdown(0);
        }
    }

    @Test
    void shutdownClosesIdleConnectionsAndReportsClean() {
        pool(cfg(db, clock, scheduler).minSize(2).maxSize(2));
        assertTrue(pool.shutdown(1_000));
        assertTrue(db.conn(0).closed);
        assertTrue(db.conn(1).closed);
        assertEquals(0, pool.metrics().total());
        assertTrue(pool.isClosed());
    }

    @Test
    void borrowingAfterShutdownFails() {
        pool(cfg(db, clock, scheduler));
        pool.shutdown(0);
        assertThrows(PoolClosedException.class, () -> pool.getConnection());
    }

    @Test
    void shutdownWaitsForALentConnectionToComeBack() throws Exception {
        pool(cfg(db, clock, scheduler).maxSize(1));
        Connection held = pool.getConnection();
        Task<Boolean> shutdown = async(() -> pool.shutdown(10_000));
        awaitTrue("pool closed", pool::isClosed);
        assertFalse(shutdown.isDone(), "shutdown must still be waiting for the lent connection");
        held.close();
        assertTrue(shutdown.get());
        assertTrue(db.conn(0).closed);
        assertEquals(0, pool.metrics().total());
    }

    @Test
    void afterTheDeadlineLentConnectionsAreClosedAndShutdownReportsFalse() throws Exception {
        pool(cfg(db, clock, scheduler).maxSize(1));
        Connection held = pool.getConnection();
        assertFalse(pool.shutdown(30));
        assertTrue(db.conn(0).closed);
        assertEquals(0, pool.metrics().total());
        assertDoesNotThrow(held::close); // the late close is harmless
        assertEquals(0, pool.metrics().total());
    }

    @Test
    void waitingBorrowersFailWithPoolClosedWhenShutdownStarts() throws Exception {
        // a long borrow timeout: the waiter must be failed by shutdown itself, not by running out its own timeout
        pool(cfg(db, clock, scheduler).maxSize(1).borrowTimeoutMillis(120_000));
        Connection held = pool.getConnection();
        Task<Connection> waiter = async(() -> pool.getConnection());
        awaitWaiting(pool, 1);
        pool.shutdown(30);
        assertInstanceOf(PoolClosedException.class, waiter.failure());
        assertEquals(0, pool.metrics().waiting());
        held.close();
    }

    @Test
    void shutdownIsIdempotent() {
        pool(cfg(db, clock, scheduler).minSize(1).maxSize(1));
        assertTrue(pool.shutdown(100));
        assertTrue(pool.shutdown(100));
        pool.close();
        assertEquals(0, pool.metrics().total());
    }

    @Test
    void closeUsesTheConfiguredShutdownTimeout() throws Exception {
        pool(cfg(db, clock, scheduler).maxSize(1).shutdownTimeoutMillis(30));
        pool.getConnection();
        pool.close();
        assertTrue(db.conn(0).closed);
    }

    @Test
    void housekeepingAfterShutdownIsHarmless() {
        pool(cfg(db, clock, scheduler).minSize(1).maxSize(1));
        pool.shutdown(0);
        assertDoesNotThrow(pool::runHousekeeping);
        assertEquals(1, db.conns.size()); // no replenishment after shutdown
    }

    @Test
    void aConnectionReturnedDuringShutdownIsClosedNotPooled() throws Exception {
        pool(cfg(db, clock, scheduler).maxSize(2));
        Connection a = pool.getConnection();
        Connection b = pool.getConnection();
        Task<Boolean> shutdown = async(() -> pool.shutdown(10_000));
        awaitTrue("pool closed", pool::isClosed);
        a.close();
        assertTrue(db.conn(0).closed);
        assertFalse(shutdown.isDone());
        b.close();
        assertTrue(shutdown.get());
        assertEquals(0, pool.metrics().idle());
    }

    @Test
    void thePoolRunsWithItsOwnThreadSchedulerWhenNoneIsGiven() throws Exception {
        pool = new JdbcPool(PoolConfig.builder(db).minSize(1).maxSize(1).housekeepingPeriodMillis(10).build());
        pool.getConnection().close();
        assertTrue(pool.shutdown(1_000));
    }
}
