package com.github.sup3rmar10.jdbcpool;

import static com.github.sup3rmar10.jdbcpool.Support.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BorrowTest {
    private final FakeDb db = new FakeDb();
    private final ManualClock clock = new ManualClock();
    private final ManualScheduler scheduler = new ManualScheduler();
    private JdbcPool pool;

    private PoolConfig.Builder cfg() {
        return Support.cfg(db, clock, scheduler);
    }

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
    void minSizeConnectionsAreOpenedAtStartup() {
        pool(cfg().minSize(2).maxSize(4));
        PoolMetrics m = pool.metrics();
        assertEquals(2, m.idle());
        assertEquals(2, m.created());
        assertEquals(2, db.conns.size());
    }

    @Test
    void connectionsAreOpenedLazilyUpToMaxAndNotBeyond() throws Exception {
        pool(cfg().maxSize(3).borrowTimeoutMillis(50));
        List<Connection> held = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            held.add(pool.getConnection());
        }
        assertEquals(3, db.conns.size());
        assertThrows(PoolTimeoutException.class, () -> pool.getConnection());
        assertEquals(3, db.conns.size());
        for (Connection c : held) {
            c.close();
        }
    }

    @Test
    void timeoutIsCountedAndTheMessageSaysWhy() throws Exception {
        pool(cfg().maxSize(1).borrowTimeoutMillis(50));
        Connection held = pool.getConnection();
        PoolTimeoutException e = assertThrows(PoolTimeoutException.class, () -> pool.getConnection());
        assertTrue(e.getMessage().contains("max=1"), e.getMessage());
        assertTrue(e.getMessage().contains("50 ms"), e.getMessage());
        assertEquals(1, pool.metrics().timeouts());
        assertEquals(0, pool.metrics().waiting());
        held.close();
    }

    @Test
    void aTimedOutBorrowDoesNotLoseOrLeakASlot() throws Exception {
        pool(cfg().maxSize(1).borrowTimeoutMillis(30));
        Connection held = pool.getConnection();
        assertThrows(PoolTimeoutException.class, () -> pool.getConnection());
        held.close();
        pool.getConnection().close();
        PoolMetrics m = pool.metrics();
        assertEquals(1, m.total());
        assertEquals(1, db.conns.size());
    }

    @Test
    void aWaiterReceivesTheConnectionThatIsReturned() throws Exception {
        pool(cfg().maxSize(1));
        Connection held = pool.getConnection();
        Task<String> waiter = async(() -> {
            try (Connection c = pool.getConnection()) {
                return c.toString();
            }
        });
        awaitWaiting(pool, 1);
        held.close();
        assertTrue(waiter.get().contains("fake-0"));
        assertEquals(1, db.conns.size());
    }

    @Test
    void waitersAreServedInArrivalOrder() throws Exception {
        pool(cfg().maxSize(1));
        Connection held = pool.getConnection();
        List<Integer> order = java.util.Collections.synchronizedList(new ArrayList<>());
        List<Task<Void>> tasks = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            int id = i;
            tasks.add(async(() -> {
                try (Connection c = pool.getConnection()) {
                    order.add(id);
                }
                return null;
            }));
            awaitWaiting(pool, i); // thread i is queued before thread i+1 starts
        }
        held.close();
        for (Task<Void> t : tasks) {
            t.get();
        }
        assertEquals(List.of(1, 2, 3, 4), order);
    }

    @Test
    void aNewArrivalCannotOvertakeAWaiter() throws Exception {
        pool(cfg().maxSize(1).borrowTimeoutMillis(100));
        Connection held = pool.getConnection();
        CountDownLatch waiterHasIt = new CountDownLatch(1);
        CountDownLatch letGo = new CountDownLatch(1);
        Task<Void> waiter = async(() -> {
            try (Connection c = pool.getConnection()) {
                waiterHasIt.countDown();
                letGo.await();
            }
            return null;
        });
        awaitWaiting(pool, 1);
        held.close();
        // the connection was handed to the waiter at close(); we must not be able to take it now
        assertThrows(PoolTimeoutException.class, () -> pool.getConnection());
        waiterHasIt.await();
        letGo.countDown();
        waiter.get();
    }

    @Test
    void creationFailureIsReportedAndTheSlotIsFreed() throws Exception {
        pool(cfg().maxSize(1));
        db.failNext.set(1);
        SQLException e = assertThrows(SQLException.class, () -> pool.getConnection());
        assertTrue(e.getMessage().contains("simulated"));
        PoolMetrics m = pool.metrics();
        assertEquals(0, m.total());
        assertEquals(1, m.createFailures());
        pool.getConnection().close();
        assertEquals(1, pool.metrics().total());
    }

    @Test
    void aFactoryThatThrowsAnUncheckedExceptionSurfacesAsSqlException() {
        pool(cfg().maxSize(1));
        JdbcPool broken = new JdbcPool(PoolConfig.builder(() -> {
            throw new IllegalStateException("kaput");
        }).minSize(0).maxSize(1).scheduler(scheduler).build());
        SQLException e = assertThrows(SQLException.class, broken::getConnection);
        assertInstanceOf(IllegalStateException.class, e.getCause());
        assertEquals(0, broken.metrics().total());
        broken.shutdown(0);
    }

    @Test
    void aFactoryReturningNullSurfacesAsSqlException() {
        JdbcPool broken = new JdbcPool(PoolConfig.builder(() -> null).minSize(0).maxSize(1).scheduler(scheduler).build());
        SQLException e = assertThrows(SQLException.class, broken::getConnection);
        assertTrue(e.getMessage().contains("null"));
        broken.shutdown(0);
    }

    @Test
    void aBrokenReturnFreesTheSlotForAWaiterWhoOpensANewConnection() throws Exception {
        pool(cfg().maxSize(1));
        Connection held = pool.getConnection();
        Task<String> waiter = async(() -> {
            try (Connection c = pool.getConnection()) {
                return c.toString();
            }
        });
        awaitWaiting(pool, 1);
        held.setAutoCommit(false);
        db.conn(0).failures.put("rollback", new SQLException("reset failed"));
        held.close(); // reset fails -> physical connection discarded -> waiter gets permission to open
        assertTrue(waiter.get().contains("fake-1"));
        assertTrue(db.conn(0).closed);
        assertEquals(1, pool.metrics().total());
    }

    @Test
    void aWaiterWhoseOpenFailsPassesTheSlotToTheNextWaiter() throws Exception {
        pool(cfg().maxSize(1));
        Connection held = pool.getConnection();
        Task<String> first = async(() -> {
            try (Connection c = pool.getConnection()) {
                return c.toString();
            }
        });
        awaitWaiting(pool, 1);
        Task<String> second = async(() -> {
            try (Connection c = pool.getConnection()) {
                return c.toString();
            }
        });
        awaitWaiting(pool, 2);
        held.setAutoCommit(false);
        db.conn(0).failures.put("rollback", new SQLException("reset failed"));
        db.failNext.set(1);
        held.close();
        Throwable failure = first.failure();
        assertTrue(failure.getMessage().contains("simulated"), String.valueOf(failure));
        assertTrue(second.get().startsWith("PooledConnection[fake-"));
        assertEquals(1, pool.metrics().total());
        assertEquals(1, pool.metrics().createFailures());
    }

    @Test
    void anInterruptedWaiterLeavesTheQueueAndKeepsItsInterruptFlag() throws Exception {
        pool(cfg().maxSize(1));
        Connection held = pool.getConnection();
        Task<Boolean> waiter = async(() -> {
            try {
                pool.getConnection();
                return false;
            } catch (SQLException e) {
                assertInstanceOf(InterruptedException.class, e.getCause());
                return Thread.currentThread().isInterrupted();
            }
        });
        awaitWaiting(pool, 1);
        waiter.thread.interrupt();
        assertTrue(waiter.get());
        assertEquals(0, pool.metrics().waiting());
        held.close();
        pool.getConnection().close(); // pool still healthy
    }

    @Test
    void idleConnectionsAreLentMostRecentlyReturnedFirst() throws Exception {
        pool(cfg().maxSize(2));
        Connection a = pool.getConnection();
        Connection b = pool.getConnection();
        a.close();
        b.close();
        try (Connection next = pool.getConnection()) {
            assertTrue(next.toString().contains("fake-1"), next.toString());
        }
    }

    @Test
    void metricsDescribeActiveIdleAndTotal() throws Exception {
        pool(cfg().maxSize(3));
        Connection a = pool.getConnection();
        Connection b = pool.getConnection();
        a.close();
        PoolMetrics m = pool.metrics();
        assertEquals(1, m.active());
        assertEquals(1, m.idle());
        assertEquals(2, m.total());
        assertEquals(2, m.created());
        b.close();
    }

    @Test
    void credentialsPerCallAreNotSupported() {
        pool(cfg());
        assertThrows(SQLFeatureNotSupportedException.class, () -> pool.getConnection("u", "p"));
    }

    @Test
    void dataSourcePlumbing() throws Exception {
        pool(cfg().borrowTimeoutMillis(7_000));
        assertEquals(7, pool.getLoginTimeout());
        assertThrows(SQLFeatureNotSupportedException.class, () -> pool.setLoginTimeout(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> pool.getParentLogger());
        assertTrue(pool.isWrapperFor(javax.sql.DataSource.class));
        assertEquals(pool, pool.unwrap(JdbcPool.class));
        assertThrows(SQLException.class, () -> pool.unwrap(String.class));
        assertNotEquals(null, pool.metrics());
    }
}
