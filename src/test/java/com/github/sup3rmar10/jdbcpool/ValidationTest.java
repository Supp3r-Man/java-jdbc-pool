package com.github.sup3rmar10.jdbcpool;

import static com.github.sup3rmar10.jdbcpool.Support.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ValidationTest {
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
    void anIdleConnectionIsValidatedWithTheConfiguredTimeout() throws Exception {
        pool(cfg(db, clock, scheduler).minSize(1).maxSize(2).validationTimeoutSeconds(3));
        pool.getConnection().close();
        assertEquals(1, db.conn(0).validCalls.get());
        assertEquals(3, db.conn(0).lastValidTimeout);
    }

    @Test
    void aFreshlyOpenedConnectionIsNotValidatedAgain() throws Exception {
        pool(cfg(db, clock, scheduler).minSize(0).maxSize(2));
        pool.getConnection().close();
        assertEquals(0, db.conn(0).validCalls.get());
    }

    @Test
    void anInvalidIdleConnectionIsClosedAndReplaced() throws Exception {
        pool(cfg(db, clock, scheduler).minSize(1).maxSize(2));
        db.conn(0).valid = false;
        try (Connection c = pool.getConnection()) {
            assertTrue(c.toString().contains("fake-1"), c.toString());
        }
        assertTrue(db.conn(0).closed);
        PoolMetrics m = pool.metrics();
        assertEquals(1, m.validationFailures());
        assertEquals(1, m.evicted());
        assertEquals(2, m.created());
        assertEquals(1, m.total());
    }

    @Test
    void anExceptionFromIsValidCountsAsInvalid() throws Exception {
        pool(cfg(db, clock, scheduler).minSize(1).maxSize(2));
        db.conn(0).failures.put("isValid", new SQLException("driver exploded"));
        try (Connection c = pool.getConnection()) {
            assertTrue(c.toString().contains("fake-1"));
        }
        assertEquals(1, pool.metrics().validationFailures());
    }

    @Test
    void withValidationOffABadConnectionIsHandedOut() throws Exception {
        pool(cfg(db, clock, scheduler).minSize(1).maxSize(2).validateOnBorrow(false));
        db.conn(0).valid = false;
        try (Connection c = pool.getConnection()) {
            assertTrue(c.toString().contains("fake-0"));
        }
        assertEquals(0, db.conn(0).validCalls.get());
        assertEquals(0, pool.metrics().validationFailures());
    }

    @Test
    void ifTheReplacementCannotBeOpenedTheErrorSurfacesAndTheSlotIsFree() throws Exception {
        pool(cfg(db, clock, scheduler).minSize(1).maxSize(1));
        db.conn(0).valid = false;
        db.failNext.set(1);
        assertThrows(SQLException.class, () -> pool.getConnection());
        assertTrue(db.conn(0).closed);
        PoolMetrics m = pool.metrics();
        assertEquals(0, m.total());
        assertEquals(1, m.createFailures());
        try (Connection c = pool.getConnection()) {
            assertFalse(c.isClosed());
        }
        assertEquals(1, pool.metrics().total());
    }

    @Test
    void aWaiterHandedABadConnectionGetsAReplacementInPlace() throws Exception {
        pool(cfg(db, clock, scheduler).minSize(0).maxSize(1));
        Connection held = pool.getConnection();
        Task<String> waiter = async(() -> {
            try (Connection c = pool.getConnection()) {
                return c.toString();
            }
        });
        awaitWaiting(pool, 1);
        db.conn(0).valid = false; // dies while it is being handed over
        held.close();
        assertTrue(waiter.get().contains("fake-1"));
        assertTrue(db.conn(0).closed);
        assertEquals(1, pool.metrics().total());
        assertEquals(1, pool.metrics().validationFailures());
    }

    @Test
    void aConnectionThatBecomesInvalidWhileLentIsStillReturnedToTheIdleQueue() throws Exception {
        // the pool does not validate on return; it validates on the next borrow
        pool(cfg(db, clock, scheduler).minSize(0).maxSize(1));
        Connection c = pool.getConnection();
        db.conn(0).valid = false;
        c.close();
        assertEquals(1, pool.metrics().idle());
        assertEquals(0, db.conn(0).validCalls.get());
        try (Connection next = pool.getConnection()) {
            assertTrue(next.toString().contains("fake-1"));
        }
    }
}
