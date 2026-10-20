package com.github.sup3rmar10.jdbcpool;

import static com.github.sup3rmar10.jdbcpool.Support.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Many real threads against a small pool. The assertions are invariants, not exact interleavings:
 * never more than max open, every borrow ends in exactly one outcome, nothing is lost.
 */
class StressTest {
    private static void assertQuiescent(JdbcPool pool, FakeDb db, int max) {
        PoolMetrics m = pool.metrics();
        assertEquals(0, m.active(), "nothing may still be lent: " + m);
        assertEquals(0, m.waiting(), "nobody may still be waiting: " + m);
        assertEquals(m.total(), m.idle(), "every accounted connection must be idle: " + m);
        assertEquals(m.total(), db.live.get(), "pool total must equal open physical connections: " + m);
        assertEquals(m.total(), m.created() - m.evicted(), "created - evicted must equal total: " + m);
        assertTrue(db.peakLive.get() <= max, "peak open physical connections " + db.peakLive.get() + " > max " + max);
        assertTrue(m.total() <= max);
    }

    @Test
    void manyThreadsNeverExceedMaxAndEveryBorrowSucceedsWithAGenerousTimeout() throws Exception {
        int max = 4;
        int threads = 16;
        int iterations = 300;
        FakeDb db = new FakeDb();
        JdbcPool pool = new JdbcPool(cfg(db, new ManualClock(), new ManualScheduler()).maxSize(max)
                .borrowTimeoutMillis(8_000).build());
        AtomicInteger inUse = new AtomicInteger();
        AtomicInteger peakInUse = new AtomicInteger();
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger timeouts = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        List<Task<Void>> tasks = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            tasks.add(async(() -> {
                start.await();
                for (int i = 0; i < iterations; i++) {
                    try {
                        Connection c = pool.getConnection();
                        peakInUse.accumulateAndGet(inUse.incrementAndGet(), Math::max);
                        Thread.yield();
                        inUse.decrementAndGet();
                        c.close();
                        ok.incrementAndGet();
                    } catch (PoolTimeoutException e) {
                        timeouts.incrementAndGet();
                    }
                }
                return null;
            }));
        }
        start.countDown();
        for (Task<Void> t : tasks) {
            t.get();
        }
        assertEquals(threads * iterations, ok.get() + timeouts.get());
        assertEquals(0, timeouts.get());
        assertTrue(peakInUse.get() <= max, "lent at once: " + peakInUse.get());
        assertEquals(timeouts.get(), pool.metrics().timeouts());
        assertQuiescent(pool, db, max);
        pool.shutdown(1_000);
    }

    @Test
    void whenEveryConnectionIsHeldEveryBorrowerTimesOutAndNoneIsLeftWaiting() throws Exception {
        int max = 3;
        FakeDb db = new FakeDb();
        JdbcPool pool = new JdbcPool(cfg(db, new ManualClock(), new ManualScheduler()).maxSize(max)
                .borrowTimeoutMillis(40).build());
        List<Connection> holders = new ArrayList<>();
        for (int i = 0; i < max; i++) {
            holders.add(pool.getConnection());
        }
        List<Task<Boolean>> borrowers = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            borrowers.add(async(() -> {
                try {
                    pool.getConnection().close();
                    return true;
                } catch (PoolTimeoutException e) {
                    return false;
                }
            }));
        }
        for (Task<Boolean> b : borrowers) {
            assertEquals(false, b.get());
        }
        assertEquals(12, pool.metrics().timeouts());
        assertEquals(0, pool.metrics().waiting());
        for (Connection c : holders) {
            c.close();
        }
        // after release the same pool serves everybody
        List<Task<Boolean>> second = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            second.add(async(() -> {
                pool.getConnection().close();
                return true;
            }));
        }
        for (Task<Boolean> b : second) {
            assertEquals(true, b.get());
        }
        assertQuiescent(pool, db, max);
        pool.shutdown(1_000);
    }

    @Test
    void poisonedBrokenAndUnopenableConnectionsNeverBreakTheAccounting() throws Exception {
        int max = 3;
        int threads = 12;
        int iterations = 200;
        FakeDb db = new FakeDb();
        JdbcPool pool = new JdbcPool(cfg(db, new ManualClock(), new ManualScheduler()).maxSize(max)
                .borrowTimeoutMillis(8_000).build());
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger timeouts = new AtomicInteger();
        AtomicInteger openFailures = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        List<Task<Void>> tasks = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            long seed = 1000 + t;
            tasks.add(async(() -> {
                Random rnd = new Random(seed);
                start.await();
                for (int i = 0; i < iterations; i++) {
                    try {
                        Connection c = pool.getConnection();
                        int id = Integer.parseInt(c.toString().replaceAll("\\D+", ""));
                        int roll = rnd.nextInt(30);
                        if (roll < 3) {
                            db.conn(id).valid = false;              // will fail validation at the next borrow
                        } else if (roll < 5) {
                            db.conn(id).failures.put("rollback", new SQLException("reset will fail"));
                            c.setAutoCommit(false);                 // forces a rollback at return, which fails
                        } else if (roll < 6) {
                            db.failNext.incrementAndGet();          // the next open attempt fails
                        }
                        c.close();
                        ok.incrementAndGet();
                    } catch (PoolTimeoutException e) {
                        timeouts.incrementAndGet();
                    } catch (SQLException e) {
                        openFailures.incrementAndGet();
                    }
                }
                return null;
            }));
        }
        start.countDown();
        for (Task<Void> t : tasks) {
            t.get();
        }
        assertEquals(threads * iterations, ok.get() + timeouts.get() + openFailures.get());
        assertEquals(timeouts.get(), pool.metrics().timeouts());
        assertEquals(openFailures.get(), pool.metrics().createFailures());
        assertTrue(pool.metrics().evicted() > 0, "the chaos should have caused at least one eviction");
        assertQuiescent(pool, db, max);
        pool.shutdown(1_000);
    }

    @Test
    void sharedH2ConnectionsLoseNoUpdatesAndAbandonedTransactionsLeaveNoTrace() throws Exception {
        int max = 3;
        int threads = 6;
        int iterations = 60;
        String url = "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000";
        JdbcPool pool = new JdbcPool(PoolConfig.builder(() -> DriverManager.getConnection(url, "sa", ""))
                .minSize(0).maxSize(max).borrowTimeoutMillis(10_000).scheduler(new ManualScheduler()).build());
        try (Connection c = pool.getConnection(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE counter (id INT PRIMARY KEY, n INT)");
            s.execute("INSERT INTO counter VALUES (1, 0)");
        }
        AtomicInteger committed = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        List<Task<Void>> tasks = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            tasks.add(async(() -> {
                start.await();
                for (int i = 0; i < iterations; i++) {
                    try (Connection c = pool.getConnection(); Statement s = c.createStatement()) {
                        if (i % 5 == 0) {
                            c.setAutoCommit(false); // abandoned: must be rolled back by the pool on close
                            s.executeUpdate("UPDATE counter SET n = n + 1000000 WHERE id = 1");
                        } else {
                            s.executeUpdate("UPDATE counter SET n = n + 1 WHERE id = 1");
                            committed.incrementAndGet();
                        }
                    }
                }
                return null;
            }));
        }
        start.countDown();
        for (Task<Void> t : tasks) {
            t.get();
        }
        try (Connection c = pool.getConnection(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT n FROM counter WHERE id = 1")) {
            rs.next();
            assertEquals(committed.get(), rs.getInt(1));
        }
        PoolMetrics m = pool.metrics();
        assertEquals(0, m.active());
        assertTrue(m.total() <= max);
        assertEquals(0, m.timeouts());
        pool.shutdown(1_000);
    }
}
