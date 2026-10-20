package com.github.sup3rmar10.jdbcpool;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Default scheduler: one daemon thread. The pool creates and shuts down its own instance unless you pass one. */
public final class ThreadScheduler implements Scheduler, AutoCloseable {
    private final ScheduledThreadPoolExecutor executor;

    public ThreadScheduler() {
        executor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "jdbc-pool-housekeeper");
            t.setDaemon(true);
            return t;
        });
        executor.setRemoveOnCancelPolicy(true);
    }

    @Override
    public Cancellable scheduleAtFixedRate(Runnable task, long periodMillis) {
        ScheduledFuture<?> f = executor.scheduleAtFixedRate(task, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
        return () -> f.cancel(false);
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
