package com.github.sup3rmar10.jdbcpool;

/** Runs the pool's housekeeping task periodically. Tests replace it with a scheduler they tick by hand. */
public interface Scheduler {
    Cancellable scheduleAtFixedRate(Runnable task, long periodMillis);

    interface Cancellable {
        void cancel();
    }
}
