package com.github.sup3rmar10.jdbcpool;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Scripted tour of the pool against in-memory H2. Real clock and real threads, so exact numbers vary per run. */
public final class Demo {
    private Demo() {
    }

    public static void main(String[] args) throws Exception {
        run(System.out);
    }

    public static void run(PrintStream out) throws Exception {
        contention(out);
        leak(out);
        poisoned(out);
        crudeThroughput(out);
    }

    private static void contention(PrintStream out) throws Exception {
        out.println("== 1. contention: 12 threads, 3 connections");
        String url = "jdbc:h2:mem:demo-contention;DB_CLOSE_DELAY=-1";
        PoolConfig config = PoolConfig.builder(url, "sa", "").minSize(1).maxSize(3).borrowTimeoutMillis(20_000).build();
        try (JdbcPool pool = new JdbcPool(config)) {
            try (Connection c = pool.getConnection(); Statement s = c.createStatement()) {
                s.execute("CREATE TABLE item (id INT PRIMARY KEY, name VARCHAR(20))");
                s.execute("INSERT INTO item VALUES (1, 'widget'), (2, 'gadget')");
            }
            AtomicInteger holding = new AtomicInteger();
            AtomicInteger peakHolding = new AtomicInteger();
            AtomicInteger peakWaiting = new AtomicInteger();
            AtomicInteger queries = new AtomicInteger();
            Thread sampler = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    peakWaiting.accumulateAndGet(pool.metrics().waiting(), Math::max);
                    Thread.onSpinWait();
                }
            });
            sampler.setDaemon(true);
            sampler.start();
            List<Thread> threads = new ArrayList<>();
            for (int t = 0; t < 12; t++) {
                Thread th = new Thread(() -> {
                    for (int i = 0; i < 5; i++) {
                        try (Connection c = pool.getConnection(); Statement s = c.createStatement();
                             ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM item")) {
                            int now = holding.incrementAndGet();
                            peakHolding.accumulateAndGet(now, Math::max);
                            rs.next();
                            queries.incrementAndGet();
                            Thread.sleep(10); // pretend to do some work while holding the connection
                            holding.decrementAndGet();
                        } catch (SQLException | InterruptedException e) {
                            throw new IllegalStateException(e);
                        }
                    }
                }, "worker-" + t);
                threads.add(th);
                th.start();
            }
            for (Thread th : threads) {
                th.join();
            }
            sampler.interrupt();
            PoolMetrics m = pool.metrics();
            out.println("queries run: " + queries.get() + ", most connections held at once: " + peakHolding.get()
                    + " (max 3), most threads waiting at once (sampled): " + peakWaiting.get());
            out.println("metrics: " + m);
        }
    }

    private static void leak(PrintStream out) throws Exception {
        out.println("== 2. leak report: borrow, forget to close");
        CountDownLatch reported = new CountDownLatch(1);
        List<LeakReport> reports = new ArrayList<>();
        PoolConfig config = PoolConfig.builder("jdbc:h2:mem:demo-leak;DB_CLOSE_DELAY=-1", "sa", "")
                .minSize(0).maxSize(2).leakThresholdMillis(100).housekeepingPeriodMillis(20)
                .leakListener(r -> {
                    reports.add(r);
                    reported.countDown();
                }).build();
        try (JdbcPool pool = new JdbcPool(config)) {
            Connection leaked = leakyMethod(pool);
            if (!reported.await(10, TimeUnit.SECONDS)) {
                out.println("no leak report within 10 s (unexpected)");
            } else {
                LeakReport r = reports.get(0);
                out.println("leak report received after >= 100 ms; borrowing thread '" + r.threadName()
                        + "', first stack lines:");
                r.describe().lines().skip(1).limit(4).forEach(l -> out.println("  " + l.strip()));
            }
            leaked.close();
            out.println("metrics after returning it: " + pool.metrics());
        }
    }

    /** Separate method so the captured stack visibly points at the code that borrowed the connection. */
    private static Connection leakyMethod(JdbcPool pool) throws SQLException {
        return pool.getConnection();
    }

    private static void poisoned(PrintStream out) throws Exception {
        out.println("== 3. poisoned connection replaced");
        List<Connection> physical = new ArrayList<>();
        PoolConfig config = PoolConfig.builder(() -> {
            Connection c = DriverManager.getConnection("jdbc:h2:mem:demo-poison;DB_CLOSE_DELAY=-1", "sa", "");
            physical.add(c);
            return c;
        }).minSize(1).maxSize(1).build();
        try (JdbcPool pool = new JdbcPool(config)) {
            try (Connection c = pool.getConnection()) {
                out.println("first borrow ok, physical connections opened so far: " + physical.size());
            }
            physical.get(0).close(); // simulate the server (or a firewall) killing the connection while it sits idle
            try (Connection c = pool.getConnection(); Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT 42")) {
                rs.next();
                out.println("second borrow ok, SELECT 42 -> " + rs.getInt(1));
            }
            PoolMetrics m = pool.metrics();
            out.println("physical connections opened: " + physical.size() + ", validationFailures="
                    + m.validationFailures() + ", evicted=" + m.evicted() + ", created=" + m.created());
        }
    }

    private static void crudeThroughput(PrintStream out) throws Exception {
        out.println("== 4. crude single-thread timing (one run, in-memory H2, NOT a benchmark, no warm-up control)");
        String url = "jdbc:h2:mem:demo-timing;DB_CLOSE_DELAY=-1";
        int pooledCycles = 20_000;
        int directCycles = 500;
        try (JdbcPool pool = new JdbcPool(PoolConfig.builder(url, "sa", "").minSize(1).maxSize(2).build())) {
            long t0 = System.nanoTime();
            for (int i = 0; i < pooledCycles; i++) {
                pool.getConnection().close();
            }
            double pooledPerSec = pooledCycles / ((System.nanoTime() - t0) / 1e9);
            long t1 = System.nanoTime();
            for (int i = 0; i < directCycles; i++) {
                DriverManager.getConnection(url, "sa", "").close();
            }
            double directPerSec = directCycles / ((System.nanoTime() - t1) / 1e9);
            out.printf("pool borrow+return: %.0f per second; DriverManager open+close: %.0f per second%n",
                    pooledPerSec, directPerSec);
            out.println("(Says nothing about HikariCP: it was not measured. Opening an in-memory H2 connection involves no network.)");
        }
    }
}
