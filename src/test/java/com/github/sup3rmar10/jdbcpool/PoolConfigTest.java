package com.github.sup3rmar10.jdbcpool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PoolConfigTest {
    private static PoolConfig.Builder b() {
        return PoolConfig.builder(() -> null);
    }

    @Test
    void defaultsAreSane() {
        PoolConfig c = b().build();
        assertEquals(2, c.minSize());
        assertEquals(10, c.maxSize());
        assertEquals(30_000, c.borrowTimeoutMillis());
        assertTrue(c.validateOnBorrow());
        assertEquals(1, c.validationTimeoutSeconds());
        assertEquals(0, c.keepaliveMillis());
        assertEquals(0, c.leakThresholdMillis());
        assertNull(c.scheduler());
    }

    @Test
    void minAboveMaxIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> b().minSize(5).maxSize(4).build());
    }

    @Test
    void minEqualToMaxIsAllowed() {
        assertEquals(4, b().minSize(4).maxSize(4).build().minSize());
    }

    @Test
    void maxSizeBelowOneIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> b().minSize(0).maxSize(0).build());
    }

    @Test
    void negativeMinIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> b().minSize(-1).build());
    }

    @Test
    void borrowTimeoutMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> b().borrowTimeoutMillis(0).build());
    }

    @Test
    void negativeDurationsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> b().idleTimeoutMillis(-1).build());
        assertThrows(IllegalArgumentException.class, () -> b().maxLifetimeMillis(-1).build());
        assertThrows(IllegalArgumentException.class, () -> b().keepaliveMillis(-1).build());
        assertThrows(IllegalArgumentException.class, () -> b().leakThresholdMillis(-1).build());
        assertThrows(IllegalArgumentException.class, () -> b().shutdownTimeoutMillis(-1).build());
        assertThrows(IllegalArgumentException.class, () -> b().housekeepingPeriodMillis(0).build());
    }

    @Test
    void validationTimeoutBelowOneSecondIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> b().validationTimeoutSeconds(0).build());
    }

    @Test
    void jitterMustBeSmallerThanLifetime() {
        assertThrows(IllegalArgumentException.class,
                () -> b().maxLifetimeMillis(1000).maxLifetimeJitterMillis(1000).build());
    }

    @Test
    void defaultJitterIsTwoAndAHalfPercentOfLifetime() {
        assertEquals(1000, b().maxLifetimeMillis(40_000).build().maxLifetimeJitterMillis());
    }

    @Test
    void jitterIsNotValidatedWhenLifetimeIsDisabled() {
        PoolConfig c = b().maxLifetimeMillis(0).maxLifetimeJitterMillis(5_000).build();
        assertEquals(0, c.maxLifetimeMillis());
        assertFalse(c.maxLifetimeJitterMillis() < 0);
    }

    @Test
    void nullFactoryIsRejected() {
        assertThrows(NullPointerException.class, () -> PoolConfig.builder((ConnectionFactory) null));
    }
}
