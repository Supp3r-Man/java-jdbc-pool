package com.github.sup3rmar10.jdbcpool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.ArrayList;

/** Test doubles: a manual clock and scheduler, a scriptable fake JDBC layer, and thread helpers. */
final class Support {
    private Support() {
    }

    static final class ManualClock implements Clock {
        private final AtomicLong now = new AtomicLong(1_000);

        @Override
        public long millis() {
            return now.get();
        }

        void advance(long ms) {
            now.addAndGet(ms);
        }
    }

    static final class ManualScheduler implements Scheduler {
        Runnable task;
        long period = -1;
        volatile boolean cancelled;
        int scheduledCount;

        @Override
        public Cancellable scheduleAtFixedRate(Runnable task, long periodMillis) {
            this.task = task;
            this.period = periodMillis;
            scheduledCount++;
            return () -> cancelled = true;
        }

        /** Runs one housekeeping pass, the way the real scheduler would at each period. */
        void tick() {
            if (!cancelled) {
                task.run();
            }
        }
    }

    /** One fake physical connection. Records every call and can be told to fail. */
    static final class FakeConn implements InvocationHandler {
        final int id;
        final FakeDb db;
        volatile boolean autoCommit;
        volatile boolean readOnly;
        volatile int isolation = Connection.TRANSACTION_READ_COMMITTED;
        volatile boolean closed;
        volatile boolean valid = true;
        volatile int lastValidTimeout = -1;
        final AtomicInteger validCalls = new AtomicInteger();
        final List<String> calls = Collections.synchronizedList(new ArrayList<>());
        final Map<String, SQLException> failures = new ConcurrentHashMap<>();
        final List<FakeStmt> statements = new CopyOnWriteArrayList<>();
        final Connection proxy;

        FakeConn(int id, FakeDb db, boolean autoCommit) {
            this.id = id;
            this.db = db;
            this.autoCommit = autoCommit;
            this.proxy = (Connection) Proxy.newProxyInstance(Support.class.getClassLoader(),
                    new Class<?>[]{Connection.class}, this);
        }

        boolean called(String prefix) {
            synchronized (calls) {
                return calls.stream().anyMatch(c -> c.startsWith(prefix));
            }
        }

        int indexOf(String call) {
            return calls.indexOf(call);
        }

        @Override
        public Object invoke(Object p, java.lang.reflect.Method m, Object[] args) throws Throwable {
            String n = m.getName();
            if (m.getDeclaringClass() == Object.class) {
                return switch (n) {
                    case "equals" -> p == args[0];
                    case "hashCode" -> id;
                    default -> "fake-" + id;
                };
            }
            calls.add(args == null ? n : n + Arrays.toString(args));
            SQLException failure = failures.get(n);
            if (failure != null) {
                throw failure;
            }
            switch (n) {
                case "getAutoCommit":
                    return autoCommit;
                case "setAutoCommit":
                    autoCommit = (Boolean) args[0];
                    return null;
                case "isReadOnly":
                    return readOnly;
                case "setReadOnly":
                    readOnly = (Boolean) args[0];
                    return null;
                case "getTransactionIsolation":
                    return isolation;
                case "setTransactionIsolation":
                    isolation = (Integer) args[0];
                    return null;
                case "isValid":
                    lastValidTimeout = (Integer) args[0];
                    validCalls.incrementAndGet();
                    return !closed && valid;
                case "close":
                    if (!closed) {
                        closed = true;
                        db.live.decrementAndGet();
                    }
                    return null;
                case "isClosed":
                    return closed;
                case "createStatement":
                case "prepareStatement": {
                    FakeStmt s = new FakeStmt(m.getReturnType());
                    statements.add(s);
                    return s.proxy;
                }
                default:
                    return defaultValue(m.getReturnType());
            }
        }
    }

    static final class FakeStmt implements InvocationHandler {
        volatile boolean closed;
        final AtomicInteger closeCalls = new AtomicInteger();
        final Statement proxy;

        FakeStmt(Class<?> type) {
            this.proxy = (Statement) Proxy.newProxyInstance(Support.class.getClassLoader(),
                    new Class<?>[]{type == PreparedStatement.class ? PreparedStatement.class : Statement.class}, this);
        }

        @Override
        public Object invoke(Object p, java.lang.reflect.Method m, Object[] args) {
            switch (m.getName()) {
                case "close":
                    closed = true;
                    closeCalls.incrementAndGet();
                    return null;
                case "isClosed":
                    return closed;
                case "toString":
                    return "fake-statement";
                case "hashCode":
                    return System.identityHashCode(this);
                case "equals":
                    return p == args[0];
                default:
                    return defaultValue(m.getReturnType());
            }
        }
    }

    static Object defaultValue(Class<?> t) {
        if (t == boolean.class) return false;
        if (t == int.class) return 0;
        if (t == long.class) return 0L;
        return null;
    }

    /** Factory of fake connections that also tracks how many are open at once. */
    static final class FakeDb implements ConnectionFactory {
        final List<FakeConn> conns = new CopyOnWriteArrayList<>();
        final AtomicInteger failNext = new AtomicInteger();
        final AtomicInteger live = new AtomicInteger();
        final AtomicInteger peakLive = new AtomicInteger();
        volatile boolean defaultAutoCommit = true;

        @Override
        public Connection open() throws SQLException {
            if (failNext.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                throw new SQLException("simulated open failure");
            }
            FakeConn c;
            synchronized (this) {
                c = new FakeConn(conns.size(), this, defaultAutoCommit);
                conns.add(c);
            }
            peakLive.accumulateAndGet(live.incrementAndGet(), Math::max);
            return c.proxy;
        }

        FakeConn conn(int i) {
            return conns.get(i);
        }
    }

    /** A builder wired to a fake database, manual clock and manual scheduler, with quiet defaults. */
    static PoolConfig.Builder cfg(FakeDb db, ManualClock clock, ManualScheduler scheduler) {
        return PoolConfig.builder(db).minSize(0).maxSize(3).borrowTimeoutMillis(5_000)
                .clock(clock).scheduler(scheduler).leakListener(r -> { });
    }

    static final class Task<T> {
        final Thread thread;
        final CompletableFuture<T> result = new CompletableFuture<>();

        Task(Callable<T> body, String name) {
            thread = new Thread(() -> {
                try {
                    result.complete(body.call());
                } catch (Throwable e) {
                    result.completeExceptionally(e);
                }
            }, name);
            thread.setDaemon(true);
            thread.start();
        }

        T get() throws Exception {
            try {
                return result.get(10, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof Exception ex) {
                    throw ex;
                }
                throw e;
            }
        }

        /** The exception the task ended with (waits for it), or null if it completed normally. */
        Throwable failure() throws Exception {
            try {
                result.get(10, TimeUnit.SECONDS);
                return null;
            } catch (ExecutionException e) {
                return e.getCause();
            } catch (TimeoutException e) {
                throw new AssertionError("task did not finish within 10 s", e);
            }
        }

        boolean isDone() {
            return result.isDone();
        }
    }

    static <T> Task<T> async(Callable<T> body) {
        return new Task<>(body, "test-task");
    }

    /** Polls a condition (no fixed sleeps) and fails if it does not become true within 5 s. */
    static void awaitTrue(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for: " + what);
            }
            Thread.sleep(1);
        }
    }

    static void awaitWaiting(JdbcPool pool, int n) throws InterruptedException {
        awaitTrue("waiting == " + n, () -> pool.metrics().waiting() == n);
    }
}
