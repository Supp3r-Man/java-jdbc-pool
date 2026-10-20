package com.github.sup3rmar10.jdbcpool;

import static com.github.sup3rmar10.jdbcpool.Support.ManualScheduler;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The same pool against a real JDBC driver (H2, in memory). A pool of size 1 makes "same physical connection" certain. */
class H2IntegrationTest {
    private final String url = "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";
    private final List<Connection> physical = new ArrayList<>();
    private JdbcPool pool;

    @BeforeEach
    void setUp() throws Exception {
        pool = new JdbcPool(PoolConfig.builder(() -> {
            Connection c = DriverManager.getConnection(url, "sa", "");
            physical.add(c);
            return c;
        }).minSize(0).maxSize(1).borrowTimeoutMillis(2_000).scheduler(new ManualScheduler()).build());
        try (Connection c = pool.getConnection(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t (id INT PRIMARY KEY, label VARCHAR(20))");
        }
    }

    @AfterEach
    void tearDown() {
        pool.shutdown(0);
    }

    private int count() throws Exception {
        try (Connection c = pool.getConnection(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM t")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Test
    void ordinaryQueriesWorkThroughThePool() throws Exception {
        try (Connection c = pool.getConnection(); Statement s = c.createStatement()) {
            s.execute("INSERT INTO t VALUES (1, 'widget')");
        }
        try (Connection c = pool.getConnection(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT label FROM t WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals("widget", rs.getString(1));
        }
        assertEquals(1, physical.size());
    }

    @Test
    void uncommittedWorkIsRolledBackWhenTheConnectionIsReturned() throws Exception {
        try (Connection c = pool.getConnection()) {
            c.setAutoCommit(false);
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (1, 'abandoned')");
            }
        } // closed without commit
        assertEquals(0, count());
        assertEquals(1, physical.size(), "must be the same physical connection, or the test proves nothing");
    }

    @Test
    void committedWorkSurvivesTheReturn() throws Exception {
        try (Connection c = pool.getConnection()) {
            c.setAutoCommit(false);
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO t VALUES (1, 'kept')");
            }
            c.commit();
        }
        assertEquals(1, count());
    }

    @Test
    void autoCommitReadOnlyAndIsolationAreResetForTheNextBorrower() throws Exception {
        int defaultIsolation;
        try (Connection c = pool.getConnection()) {
            defaultIsolation = c.getTransactionIsolation();
            c.setAutoCommit(false);
            c.setReadOnly(true);
            c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        }
        try (Connection c = pool.getConnection()) {
            assertTrue(c.getAutoCommit());
            assertFalse(c.isReadOnly());
            assertEquals(defaultIsolation, c.getTransactionIsolation());
        }
        assertEquals(1, physical.size());
    }

    @Test
    void openStatementsAndResultSetsAreClosedOnReturn() throws Exception {
        Statement s;
        ResultSet rs;
        try (Connection c = pool.getConnection()) {
            s = c.createStatement();
            rs = s.executeQuery("SELECT * FROM t");
            assertFalse(s.isClosed());
        }
        assertTrue(s.isClosed());
        assertTrue(rs.isClosed());
    }

    @Test
    void aConnectionKilledWhileIdleIsReplacedOnTheNextBorrow() throws Exception {
        pool.getConnection().close();
        physical.get(0).close(); // the "server" drops it
        try (Connection c = pool.getConnection(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT 42")) {
            rs.next();
            assertEquals(42, rs.getInt(1));
        }
        assertEquals(2, physical.size());
        PoolMetrics m = pool.metrics();
        assertEquals(1, m.validationFailures());
        assertEquals(1, m.total());
    }

    @Test
    void aConnectionKilledWhileLentIsDiscardedOnReturn() throws Exception {
        Connection c = pool.getConnection();
        physical.get(0).close(); // dies underneath the caller
        c.close();               // the rollback/reset check fails, so it must not go back to idle as if healthy
        try (Connection next = pool.getConnection(); Statement s = next.createStatement()) {
            s.execute("SELECT 1");
        }
        assertEquals(2, physical.size());
    }
}
