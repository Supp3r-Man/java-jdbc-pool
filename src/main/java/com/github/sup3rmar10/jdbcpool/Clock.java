package com.github.sup3rmar10.jdbcpool;

/**
 * Source of "now" for everything the pool decides by age: idle time, max lifetime, keepalive and leak
 * thresholds. Tests inject a manual clock. Borrow timeouts are the one exception: they are measured with
 * {@link System#nanoTime()} because a waiting thread has to be woken by the JVM, not by a fake clock.
 */
@FunctionalInterface
public interface Clock {
    /** Milliseconds on a monotonic timeline (only differences are meaningful). */
    long millis();

    Clock SYSTEM = () -> System.nanoTime() / 1_000_000L;
}
