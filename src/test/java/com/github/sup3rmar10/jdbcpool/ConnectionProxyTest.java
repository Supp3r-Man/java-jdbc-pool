package com.github.sup3rmar10.jdbcpool;

import static com.github.sup3rmar10.jdbcpool.Support.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectionProxyTest {
    private final FakeDb db = new FakeDb();
    private final ManualClock clock = new ManualClock();
    private final ManualScheduler scheduler = new ManualScheduler();
    private JdbcPool pool;

    @BeforeEach
    void setUp() {
        pool = new JdbcPool(cfg(db, clock, scheduler).maxSize(1).borrowTimeoutMillis(50).build());
    }

    @AfterEach
    void tearDown() {
        pool.shutdown(0);
    }

    @Test
    void closeReturnsTheConnectionInsteadOfClosingIt() throws Exception {
        Connection c = pool.getConnection();
        c.close();
        assertFalse(db.conn(0).closed);
        PoolMetrics m = pool.metrics();
        assertEquals(1, m.idle());
        assertEquals(0, m.active());
    }

    @Test
    void theSamePhysicalConnectionIsReused() throws Exception {
        pool.getConnection().close();
        pool.getConnection().close();
        assertEquals(1, db.conns.size());
    }

    @Test
    void doubleCloseDoesNotPutTheConnectionInTheIdleQueueTwice() throws Exception {
        Connection c = pool.getConnection();
        c.close();
        c.close();
        assertEquals(1, pool.metrics().idle());
        Connection again = pool.getConnection();
        // with max size 1 a duplicate idle entry would let a second borrow succeed
        assertThrows(PoolTimeoutException.class, () -> pool.getConnection());
        again.close();
    }

    @Test
    void useAfterCloseThrows() throws Exception {
        Connection c = pool.getConnection();
        c.close();
        SQLException e = assertThrows(SQLException.class, c::createStatement);
        assertTrue(e.getMessage().contains("closed"));
        assertThrows(SQLException.class, c::commit);
        assertThrows(SQLException.class, () -> c.setAutoCommit(false));
    }

    @Test
    void isClosedReflectsTheLogicalState() throws Exception {
        Connection c = pool.getConnection();
        assertFalse(c.isClosed());
        c.close();
        assertTrue(c.isClosed());
        assertFalse(db.conn(0).closed);
    }

    @Test
    void aStaleHandleCannotReachTheNextBorrowersConnection() throws Exception {
        Connection stale = pool.getConnection();
        stale.close();
        Connection fresh = pool.getConnection();
        int before = db.conn(0).calls.size();
        assertThrows(SQLException.class, () -> stale.setReadOnly(true));
        assertEquals(before, db.conn(0).calls.size());
        assertFalse(db.conn(0).readOnly);
        fresh.close();
    }

    @Test
    void uncommittedWorkIsRolledBackBeforeAutoCommitIsRestored() throws Exception {
        Connection c = pool.getConnection();
        c.setAutoCommit(false);
        c.close();
        FakeConn f = db.conn(0);
        int rollback = f.indexOf("rollback");
        int restore = f.indexOf("setAutoCommit[true]");
        assertTrue(rollback >= 0, f.calls.toString());
        assertTrue(restore > rollback, "rollback must precede setAutoCommit(true): " + f.calls);
        assertTrue(f.autoCommit);
    }

    @Test
    void noRollbackIsIssuedWhenAutoCommitIsOn() throws Exception {
        pool.getConnection().close();
        assertFalse(db.conn(0).called("rollback"));
    }

    @Test
    void readOnlyIsRestored() throws Exception {
        Connection c = pool.getConnection();
        c.setReadOnly(true);
        assertTrue(db.conn(0).readOnly);
        c.close();
        assertFalse(db.conn(0).readOnly);
    }

    @Test
    void transactionIsolationIsRestored() throws Exception {
        Connection c = pool.getConnection();
        c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        c.close();
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, db.conn(0).isolation);
    }

    @Test
    void settingsTheCallerDidNotChangeAreNotRewritten() throws Exception {
        pool.getConnection().close();
        FakeConn f = db.conn(0);
        assertFalse(f.called("setAutoCommit"));
        assertFalse(f.called("setReadOnly"));
        assertFalse(f.called("setTransactionIsolation"));
    }

    @Test
    void warningsAreCleared() throws Exception {
        pool.getConnection().close();
        assertTrue(db.conn(0).called("clearWarnings"));
    }

    @Test
    void defaultsAreCapturedFromTheConnectionNotAssumed() throws Exception {
        pool.shutdown(0);
        FakeDb odd = new FakeDb();
        odd.defaultAutoCommit = false;
        pool = new JdbcPool(cfg(odd, clock, scheduler).maxSize(1).build());
        Connection c = pool.getConnection();
        c.setAutoCommit(true);
        c.close();
        assertFalse(odd.conn(0).autoCommit);
    }

    @Test
    void openStatementsAreClosedOnReturn() throws Exception {
        Connection c = pool.getConnection();
        c.createStatement();
        c.prepareStatement("select 1");
        c.close();
        assertEquals(2, db.conn(0).statements.size());
        assertTrue(db.conn(0).statements.stream().allMatch(s -> s.closed));
    }

    @Test
    void aStatementClosedByTheCallerIsNotClosedAgain() throws Exception {
        Connection c = pool.getConnection();
        Statement s = c.createStatement();
        s.close();
        c.close();
        assertEquals(1, db.conn(0).statements.get(0).closeCalls.get());
    }

    @Test
    void statementsReportTheProxyAsTheirConnection() throws Exception {
        Connection c = pool.getConnection();
        Statement s = c.createStatement();
        assertSame(c, s.getConnection());
        c.close();
    }

    @Test
    void aFailedResetDiscardsThePhysicalConnection() throws Exception {
        Connection c = pool.getConnection();
        c.setAutoCommit(false);
        db.conn(0).failures.put("rollback", new SQLException("boom"));
        c.close();
        assertTrue(db.conn(0).closed);
        PoolMetrics m = pool.metrics();
        assertEquals(0, m.total());
        assertEquals(1, m.evicted());
        pool.getConnection().close();
        assertEquals(2, db.conns.size());
    }

    @Test
    void aConnectionClassSqlStateMarksTheConnectionBroken() throws Exception {
        Connection c = pool.getConnection();
        db.conn(0).failures.put("commit", new SQLException("link down", "08006"));
        assertThrows(SQLException.class, c::commit);
        db.conn(0).failures.clear();
        c.close();
        assertTrue(db.conn(0).closed);
        assertEquals(1, pool.metrics().evicted());
    }

    @Test
    void anOrdinarySqlStateDoesNotDiscardTheConnection() throws Exception {
        Connection c = pool.getConnection();
        db.conn(0).failures.put("commit", new SQLException("duplicate key", "23505"));
        assertThrows(SQLException.class, c::commit);
        db.conn(0).failures.clear();
        c.close();
        assertFalse(db.conn(0).closed);
        assertEquals(1, pool.metrics().idle());
    }

    @Test
    void unwrapReturnsTheProxyForConnectionAndIsWrapperForAgrees() throws Exception {
        Connection c = pool.getConnection();
        assertSame(c, c.unwrap(Connection.class));
        assertTrue(c.isWrapperFor(Connection.class));
        c.close();
    }

    @Test
    void proxiesHaveIdentityEqualityAndAreDistinctPerBorrow() throws Exception {
        Connection first = pool.getConnection();
        first.close();
        Connection second = pool.getConnection();
        assertNotEquals(first, second);
        assertEquals(second, second);
        second.close();
    }
}
