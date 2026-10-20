package com.github.sup3rmar10.jdbcpool;

import java.sql.DriverManager;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;

/** Immutable pool settings. Build with {@link #builder(ConnectionFactory)}. A value of 0 disables a time-based feature. */
public final class PoolConfig {
    private final ConnectionFactory factory;
    private final int minSize;
    private final int maxSize;
    private final long borrowTimeoutMillis;
    private final long idleTimeoutMillis;
    private final long maxLifetimeMillis;
    private final long maxLifetimeJitterMillis;
    private final boolean validateOnBorrow;
    private final int validationTimeoutSeconds;
    private final long keepaliveMillis;
    private final long leakThresholdMillis;
    private final long housekeepingPeriodMillis;
    private final long shutdownTimeoutMillis;
    private final Clock clock;
    private final Scheduler scheduler;
    private final LongUnaryOperator jitterSource;
    private final Consumer<LeakReport> leakListener;

    private PoolConfig(Builder b) {
        this.factory = b.factory;
        this.minSize = b.minSize;
        this.maxSize = b.maxSize;
        this.borrowTimeoutMillis = b.borrowTimeoutMillis;
        this.idleTimeoutMillis = b.idleTimeoutMillis;
        this.maxLifetimeMillis = b.maxLifetimeMillis;
        this.maxLifetimeJitterMillis = b.maxLifetimeJitterMillis < 0 ? b.maxLifetimeMillis / 40 : b.maxLifetimeJitterMillis;
        this.validateOnBorrow = b.validateOnBorrow;
        this.validationTimeoutSeconds = b.validationTimeoutSeconds;
        this.keepaliveMillis = b.keepaliveMillis;
        this.leakThresholdMillis = b.leakThresholdMillis;
        this.housekeepingPeriodMillis = b.housekeepingPeriodMillis;
        this.shutdownTimeoutMillis = b.shutdownTimeoutMillis;
        this.clock = b.clock;
        this.scheduler = b.scheduler;
        this.jitterSource = b.jitterSource;
        this.leakListener = b.leakListener;
    }

    public static Builder builder(ConnectionFactory factory) {
        return new Builder(factory);
    }

    /** Convenience: a factory over {@link DriverManager}. */
    public static Builder builder(String jdbcUrl, String user, String password) {
        Objects.requireNonNull(jdbcUrl, "jdbcUrl");
        return new Builder(() -> DriverManager.getConnection(jdbcUrl, user, password));
    }

    public ConnectionFactory factory() { return factory; }
    public int minSize() { return minSize; }
    public int maxSize() { return maxSize; }
    public long borrowTimeoutMillis() { return borrowTimeoutMillis; }
    public long idleTimeoutMillis() { return idleTimeoutMillis; }
    public long maxLifetimeMillis() { return maxLifetimeMillis; }
    public long maxLifetimeJitterMillis() { return maxLifetimeJitterMillis; }
    public boolean validateOnBorrow() { return validateOnBorrow; }
    public int validationTimeoutSeconds() { return validationTimeoutSeconds; }
    public long keepaliveMillis() { return keepaliveMillis; }
    public long leakThresholdMillis() { return leakThresholdMillis; }
    public long housekeepingPeriodMillis() { return housekeepingPeriodMillis; }
    public long shutdownTimeoutMillis() { return shutdownTimeoutMillis; }
    public Clock clock() { return clock; }
    /** May be null: the pool then creates (and later stops) its own {@link ThreadScheduler}. */
    public Scheduler scheduler() { return scheduler; }
    public LongUnaryOperator jitterSource() { return jitterSource; }
    public Consumer<LeakReport> leakListener() { return leakListener; }

    public static final class Builder {
        private final ConnectionFactory factory;
        private int minSize = 2;
        private int maxSize = 10;
        private long borrowTimeoutMillis = 30_000;
        private long idleTimeoutMillis = 600_000;
        private long maxLifetimeMillis = 1_800_000;
        private long maxLifetimeJitterMillis = -1; // -1 = 2.5% of the max lifetime
        private boolean validateOnBorrow = true;
        private int validationTimeoutSeconds = 1;
        private long keepaliveMillis = 0;
        private long leakThresholdMillis = 0;
        private long housekeepingPeriodMillis = 5_000;
        private long shutdownTimeoutMillis = 10_000;
        private Clock clock = Clock.SYSTEM;
        private Scheduler scheduler;
        private LongUnaryOperator jitterSource = bound -> ThreadLocalRandom.current().nextLong(bound);
        private Consumer<LeakReport> leakListener = r -> System.err.println(r.describe());

        private Builder(ConnectionFactory factory) {
            this.factory = Objects.requireNonNull(factory, "factory");
        }

        public Builder minSize(int v) { minSize = v; return this; }
        public Builder maxSize(int v) { maxSize = v; return this; }
        public Builder borrowTimeoutMillis(long v) { borrowTimeoutMillis = v; return this; }
        public Builder idleTimeoutMillis(long v) { idleTimeoutMillis = v; return this; }
        public Builder maxLifetimeMillis(long v) { maxLifetimeMillis = v; return this; }
        /** Each connection's lifetime is the max lifetime minus a value in [0, jitter]. */
        public Builder maxLifetimeJitterMillis(long v) { maxLifetimeJitterMillis = v; return this; }
        public Builder validateOnBorrow(boolean v) { validateOnBorrow = v; return this; }
        public Builder validationTimeoutSeconds(int v) { validationTimeoutSeconds = v; return this; }
        public Builder keepaliveMillis(long v) { keepaliveMillis = v; return this; }
        public Builder leakThresholdMillis(long v) { leakThresholdMillis = v; return this; }
        public Builder housekeepingPeriodMillis(long v) { housekeepingPeriodMillis = v; return this; }
        public Builder shutdownTimeoutMillis(long v) { shutdownTimeoutMillis = v; return this; }
        public Builder clock(Clock v) { clock = Objects.requireNonNull(v); return this; }
        public Builder scheduler(Scheduler v) { scheduler = v; return this; }
        /** Given a bound, returns a value in [0, bound). Tests use it to make jitter deterministic. */
        public Builder jitterSource(LongUnaryOperator v) { jitterSource = Objects.requireNonNull(v); return this; }
        public Builder leakListener(Consumer<LeakReport> v) { leakListener = Objects.requireNonNull(v); return this; }

        public PoolConfig build() {
            if (maxSize < 1) throw new IllegalArgumentException("maxSize must be >= 1");
            if (minSize < 0) throw new IllegalArgumentException("minSize must be >= 0");
            if (minSize > maxSize) throw new IllegalArgumentException("minSize must not exceed maxSize");
            if (borrowTimeoutMillis < 1) throw new IllegalArgumentException("borrowTimeoutMillis must be >= 1");
            if (idleTimeoutMillis < 0) throw new IllegalArgumentException("idleTimeoutMillis must be >= 0");
            if (maxLifetimeMillis < 0) throw new IllegalArgumentException("maxLifetimeMillis must be >= 0");
            if (maxLifetimeJitterMillis < -1) throw new IllegalArgumentException("maxLifetimeJitterMillis must be >= 0");
            if (maxLifetimeMillis > 0 && maxLifetimeJitterMillis >= maxLifetimeMillis) {
                throw new IllegalArgumentException("maxLifetimeJitterMillis must be smaller than maxLifetimeMillis");
            }
            if (validationTimeoutSeconds < 1) throw new IllegalArgumentException("validationTimeoutSeconds must be >= 1");
            if (keepaliveMillis < 0) throw new IllegalArgumentException("keepaliveMillis must be >= 0");
            if (leakThresholdMillis < 0) throw new IllegalArgumentException("leakThresholdMillis must be >= 0");
            if (housekeepingPeriodMillis < 1) throw new IllegalArgumentException("housekeepingPeriodMillis must be >= 1");
            if (shutdownTimeoutMillis < 0) throw new IllegalArgumentException("shutdownTimeoutMillis must be >= 0");
            return new PoolConfig(this);
        }
    }
}
