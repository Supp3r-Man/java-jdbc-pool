package com.github.sup3rmar10.jdbcpool;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * A small JDBC connection pool. One lock guards all state; anything that can block (opening, validating,
 * closing a physical connection, calling the leak listener) happens outside it.
 *
 * <p>Accounting rule: {@code total} counts every connection the pool is responsible for, including ones
 * being opened, validated or pinged, and is only decreased after the physical connection has been closed.
 * That is what keeps the number of open physical connections at or below maxSize.
 *
 * <p>Fairness: when a connection is returned and threads are waiting, it is handed directly to the longest
 * waiter. A free slot (a connection that was closed while threads wait) is handed to the longest waiter as
 * a permission to open a new connection. New arrivals never overtake waiters.
 */
public final class JdbcPool implements DataSource, AutoCloseable {
    private final PoolConfig cfg;
    private final Clock clock;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition drained = lock.newCondition();
    private final ArrayDeque<PooledEntry> idle = new ArrayDeque<>();
    private final Set<PooledEntry> borrowed = new LinkedHashSet<>();
    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();
    private int total;
    private boolean closed;

    private final AtomicLong timeouts = new AtomicLong();
    private final AtomicLong created = new AtomicLong();
    private final AtomicLong evicted = new AtomicLong();
    private final AtomicLong leaks = new AtomicLong();
    private final AtomicLong validationFailures = new AtomicLong();
    private final AtomicLong createFailures = new AtomicLong();

    private final Scheduler.Cancellable housekeepingTask;
    private final ThreadScheduler ownedScheduler;
    private PrintWriter logWriter;

    private static final class Waiter {
        final Condition cond;
        PooledEntry entry;
        boolean permit;
        SQLException failure;

        Waiter(Condition cond) {
            this.cond = cond;
        }

        boolean fulfilled() {
            return entry != null || permit || failure != null;
        }
    }

    public JdbcPool(PoolConfig cfg) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.clock = cfg.clock();
        replenish();
        Scheduler scheduler = cfg.scheduler();
        if (scheduler == null) {
            ownedScheduler = new ThreadScheduler();
            scheduler = ownedScheduler;
        } else {
            ownedScheduler = null;
        }
        housekeepingTask = scheduler.scheduleAtFixedRate(this::runHousekeeping, cfg.housekeepingPeriodMillis());
    }

    // ------------------------------------------------------------------ borrowing

    @Override
    public Connection getConnection() throws SQLException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(cfg.borrowTimeoutMillis());
        Throwable stack = cfg.leakThresholdMillis() > 0 ? new Throwable("connection borrowed here") : null;
        PooledEntry entry = takeOrReserve(deadline);
        if (entry == null) {
            entry = openReserved();
        } else if (cfg.validateOnBorrow() && !isAlive(entry.raw)) {
            validationFailures.incrementAndGet();
            closeQuietly(entry.raw);
            evicted.incrementAndGet();
            entry = openReserved(); // the replacement keeps the slot of the connection it replaces
        }
        return lend(entry, stack);
    }

    /** Returns an idle or handed-over entry, or null when the caller holds a reserved slot and must open one. */
    private PooledEntry takeOrReserve(long deadlineNanos) throws SQLException {
        PooledEntry dropped = null;
        SQLException interrupted;
        lock.lock();
        try {
            ensureOpen();
            PooledEntry head = idle.pollLast();
            if (head != null) {
                return head;
            }
            if (total < cfg.maxSize() && waiters.isEmpty()) {
                total++;
                return null;
            }
            Waiter w = new Waiter(lock.newCondition());
            waiters.addLast(w);
            long remaining = deadlineNanos - System.nanoTime();
            while (!w.fulfilled()) {
                if (remaining <= 0) {
                    if (waiters.remove(w)) {
                        timeouts.incrementAndGet();
                        throw new PoolTimeoutException("Timed out after " + cfg.borrowTimeoutMillis()
                                + " ms waiting for a connection (max=" + cfg.maxSize() + ", active="
                                + borrowed.size() + ", waiting=" + waiters.size() + ")");
                    }
                    break;
                }
                try {
                    remaining = w.cond.awaitNanos(remaining);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    if (!waiters.remove(w)) {
                        // fulfilled in the meantime: pass what we were given on to somebody else
                        if (w.entry != null) {
                            if (closed) {
                                detachLocked(w.entry);
                                dropped = w.entry;
                            } else {
                                returnToIdleLocked(w.entry);
                            }
                        } else if (w.permit) {
                            total--;
                            slotFreedLocked();
                            if (total == 0) {
                                drained.signalAll();
                            }
                        }
                    }
                    interrupted = new SQLException("Interrupted while waiting for a connection", ie);
                    if (dropped != null) {
                        lock.unlock();
                        try {
                            finishRemoval(List.of(dropped), false);
                        } finally {
                            lock.lock();
                        }
                    }
                    throw interrupted;
                }
            }
            if (w.failure != null) {
                throw w.failure;
            }
            return w.entry; // null means: a slot was reserved for us
        } finally {
            lock.unlock();
        }
    }

    private PooledEntry openReserved() throws SQLException {
        try {
            return newEntry();
        } catch (SQLException | RuntimeException e) {
            createFailures.incrementAndGet();
            releaseSlots(1);
            throw e;
        }
    }

    private Connection lend(PooledEntry entry, Throwable stack) throws SQLException {
        boolean rejected;
        lock.lock();
        try {
            rejected = closed;
            if (rejected) {
                detachLocked(entry);
            } else {
                entry.borrowedAt = clock.millis();
                entry.borrowerName = Thread.currentThread().getName();
                entry.borrowStack = stack;
                entry.leakReported = false;
                borrowed.add(entry);
            }
        } finally {
            lock.unlock();
        }
        if (rejected) {
            finishRemoval(List.of(entry), false);
            throw new PoolClosedException("The pool is closed");
        }
        return ConnectionProxy.create(this, entry);
    }

    /** Called by the proxy when the caller closes the connection. */
    void release(PooledEntry entry, boolean broken) {
        boolean remove = false;
        boolean evict = false;
        lock.lock();
        try {
            borrowed.remove(entry);
            if (entry.removed) {
                return; // already closed by a forced shutdown
            }
            long now = clock.millis();
            if (closed) {
                detachLocked(entry);
                remove = true;
            } else if (broken || entry.isExpired(now)) {
                detachLocked(entry);
                remove = true;
                evict = true;
            } else {
                entry.lastReturned = now;
                entry.lastAccessed = now;
                returnToIdleLocked(entry);
            }
        } finally {
            lock.unlock();
        }
        if (remove) {
            finishRemoval(List.of(entry), evict);
        }
    }

    // ------------------------------------------------------------------ internals (lock helpers)

    private void ensureOpen() throws PoolClosedException {
        if (closed) {
            throw new PoolClosedException("The pool is closed");
        }
    }

    /** Hand the entry to the longest waiter, or park it in the idle queue. Caller holds the lock. */
    private void returnToIdleLocked(PooledEntry entry) {
        Waiter w = waiters.pollFirst();
        if (w != null) {
            w.entry = entry;
            w.cond.signal();
        } else {
            idle.addLast(entry);
        }
    }

    /** Give free capacity to waiters as permission to open a connection. Caller holds the lock. */
    private void slotFreedLocked() {
        while (!closed && total < cfg.maxSize() && !waiters.isEmpty()) {
            Waiter w = waiters.pollFirst();
            total++;
            w.permit = true;
            w.cond.signal();
        }
    }

    /** Marks the entry as no longer lent/idle. The slot is released later by {@link #finishRemoval}. */
    private boolean detachLocked(PooledEntry entry) {
        if (entry.removed) {
            return false;
        }
        entry.removed = true;
        borrowed.remove(entry);
        return true;
    }

    /** Closes the physical connections outside the lock, then frees their slots. */
    private void finishRemoval(List<PooledEntry> entries, boolean countAsEvicted) {
        if (entries.isEmpty()) {
            return;
        }
        for (PooledEntry e : entries) {
            closeQuietly(e.raw);
        }
        if (countAsEvicted) {
            evicted.addAndGet(entries.size());
        }
        releaseSlots(entries.size());
    }

    private void releaseSlots(int n) {
        lock.lock();
        try {
            total -= n;
            slotFreedLocked();
            if (total == 0) {
                drained.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    private PooledEntry newEntry() throws SQLException {
        Connection c;
        try {
            c = cfg.factory().open();
        } catch (RuntimeException e) {
            throw new SQLException("Connection factory failed: " + e, e);
        }
        if (c == null) {
            throw new SQLException("Connection factory returned null");
        }
        try {
            long now = clock.millis();
            long expiresAt = Long.MAX_VALUE;
            if (cfg.maxLifetimeMillis() > 0) {
                long jitter = cfg.maxLifetimeJitterMillis() > 0
                        ? cfg.jitterSource().applyAsLong(cfg.maxLifetimeJitterMillis() + 1) : 0;
                expiresAt = now + cfg.maxLifetimeMillis() - jitter;
            }
            PooledEntry entry = new PooledEntry(c, now, expiresAt,
                    c.getAutoCommit(), c.isReadOnly(), c.getTransactionIsolation());
            created.incrementAndGet();
            return entry;
        } catch (SQLException e) {
            closeQuietly(c);
            throw e;
        } catch (RuntimeException e) {
            closeQuietly(c);
            throw new SQLException("Could not read the new connection's defaults: " + e, e);
        }
    }

    private boolean isAlive(Connection c) {
        try {
            return c.isValid(cfg.validationTimeoutSeconds());
        } catch (SQLException | RuntimeException e) {
            return false;
        }
    }

    private static void closeQuietly(Connection c) {
        try {
            c.close();
        } catch (SQLException | RuntimeException ignored) {
            // nothing useful to do
        }
    }

    // ------------------------------------------------------------------ housekeeping

    /** Runs one housekeeping pass. Scheduled by the pool; public so tests can drive it directly. */
    public void runHousekeeping() {
        try {
            housekeepOnce();
        } catch (RuntimeException e) {
            // never let an unexpected failure kill the scheduler thread
        }
    }

    private void housekeepOnce() {
        List<PooledEntry> toEvict = new ArrayList<>();
        List<PooledEntry> toPing = new ArrayList<>();
        List<LeakReport> leakReports = new ArrayList<>();
        lock.lock();
        try {
            if (closed) {
                return;
            }
            long now = clock.millis();
            if (cfg.leakThresholdMillis() > 0) {
                for (PooledEntry e : borrowed) {
                    long held = now - e.borrowedAt;
                    if (!e.leakReported && held >= cfg.leakThresholdMillis()) {
                        e.leakReported = true;
                        leakReports.add(new LeakReport(e.borrowerName, held, e.borrowStack));
                    }
                }
            }
            for (Iterator<PooledEntry> it = idle.iterator(); it.hasNext(); ) {
                PooledEntry e = it.next();
                boolean expired = e.isExpired(now);
                boolean idleTooLong = cfg.idleTimeoutMillis() > 0
                        && total - toEvict.size() > cfg.minSize()
                        && now - e.lastReturned >= cfg.idleTimeoutMillis();
                if ((expired || idleTooLong) && detachLocked(e)) {
                    it.remove();
                    toEvict.add(e);
                }
            }
            if (cfg.keepaliveMillis() > 0) {
                for (Iterator<PooledEntry> it = idle.iterator(); it.hasNext(); ) {
                    PooledEntry e = it.next();
                    if (now - e.lastAccessed >= cfg.keepaliveMillis()) {
                        it.remove(); // out of the idle queue while it is being pinged
                        toPing.add(e);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        finishRemoval(toEvict, true);
        leaks.addAndGet(leakReports.size());
        for (LeakReport report : leakReports) {
            try {
                cfg.leakListener().accept(report);
            } catch (RuntimeException ignored) {
                // a misbehaving listener must not stop housekeeping
            }
        }
        for (PooledEntry e : toPing) {
            ping(e);
        }
        replenish();
    }

    private void ping(PooledEntry entry) {
        boolean alive = isAlive(entry.raw);
        boolean remove = false;
        lock.lock();
        try {
            if (alive && !closed) {
                entry.lastAccessed = clock.millis();
                returnToIdleLocked(entry);
            } else {
                remove = detachLocked(entry);
            }
        } finally {
            lock.unlock();
        }
        if (!alive) {
            validationFailures.incrementAndGet();
        }
        if (remove) {
            finishRemoval(List.of(entry), !alive);
        }
    }

    /** Opens connections until the pool holds minSize. Failures are counted and the pass stops. */
    private void replenish() {
        int reserved = 0;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            while (total < cfg.minSize() && waiters.isEmpty()) {
                total++;
                reserved++;
            }
        } finally {
            lock.unlock();
        }
        for (int i = 0; i < reserved; i++) {
            PooledEntry entry;
            try {
                entry = newEntry();
            } catch (SQLException | RuntimeException e) {
                createFailures.incrementAndGet();
                releaseSlots(reserved - i);
                return;
            }
            boolean reject;
            lock.lock();
            try {
                reject = closed;
                if (reject) {
                    detachLocked(entry);
                } else {
                    entry.lastReturned = clock.millis();
                    entry.lastAccessed = entry.lastReturned;
                    returnToIdleLocked(entry);
                }
            } finally {
                lock.unlock();
            }
            if (reject) {
                finishRemoval(List.of(entry), false);
            }
        }
    }

    // ------------------------------------------------------------------ shutdown and metrics

    /**
     * Stops the pool: waiting borrowers fail, idle connections close, and the call waits up to the timeout for
     * lent connections to be returned. Connections still lent after the deadline are closed underneath their
     * holders. Returns true if everything was returned in time. Safe to call more than once.
     */
    public boolean shutdown(long timeoutMillis) {
        housekeepingTask.cancel();
        if (ownedScheduler != null) {
            ownedScheduler.close();
        }
        List<PooledEntry> idleEntries = new ArrayList<>();
        lock.lock();
        try {
            if (!closed) {
                closed = true;
                for (Waiter w : waiters) {
                    w.failure = new PoolClosedException("The pool is shutting down");
                    w.cond.signal();
                }
                waiters.clear();
                for (PooledEntry e : idle) {
                    if (detachLocked(e)) {
                        idleEntries.add(e);
                    }
                }
                idle.clear();
            }
        } finally {
            lock.unlock();
        }
        finishRemoval(idleEntries, false);

        boolean clean;
        List<PooledEntry> forced = new ArrayList<>();
        lock.lock();
        try {
            long remaining = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            try {
                while (total > 0 && remaining > 0) {
                    remaining = drained.awaitNanos(remaining);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            clean = total == 0;
            if (!clean) {
                for (PooledEntry e : new ArrayList<>(borrowed)) {
                    if (detachLocked(e)) {
                        forced.add(e);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        finishRemoval(forced, false);
        return clean;
    }

    /** Shuts down with the configured shutdown timeout. */
    @Override
    public void close() {
        shutdown(cfg.shutdownTimeoutMillis());
    }

    public boolean isClosed() {
        lock.lock();
        try {
            return closed;
        } finally {
            lock.unlock();
        }
    }

    public PoolMetrics metrics() {
        int active;
        int idleCount;
        int waiting;
        int totalCount;
        lock.lock();
        try {
            active = borrowed.size();
            idleCount = idle.size();
            waiting = waiters.size();
            totalCount = total;
        } finally {
            lock.unlock();
        }
        return new PoolMetrics(active, idleCount, waiting, totalCount, timeouts.get(), created.get(), evicted.get(),
                leaks.get(), validationFailures.get(), createFailures.get());
    }

    // ------------------------------------------------------------------ DataSource plumbing

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        throw new SQLFeatureNotSupportedException("Per-call credentials are not supported by this pool");
    }

    @Override
    public PrintWriter getLogWriter() {
        return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
        this.logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("Set borrowTimeoutMillis in PoolConfig instead");
    }

    @Override
    public int getLoginTimeout() {
        return (int) TimeUnit.MILLISECONDS.toSeconds(cfg.borrowTimeoutMillis());
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Not a wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }
}
