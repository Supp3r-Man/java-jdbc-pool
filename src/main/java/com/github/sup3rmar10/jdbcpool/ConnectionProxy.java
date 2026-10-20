package com.github.sup3rmar10.jdbcpool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The object handed to callers. close() resets session state and returns the physical connection to the pool
 * instead of closing it; after close() every other call throws. One proxy is created per borrow, so a stale
 * reference from an earlier borrow can never touch a connection that has since been lent to someone else.
 */
final class ConnectionProxy implements InvocationHandler {
    private final JdbcPool pool;
    private final PooledEntry entry;
    private final Connection raw;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<Statement> openStatements = ConcurrentHashMap.newKeySet();
    private volatile boolean dirtyAutoCommit;
    private volatile boolean dirtyReadOnly;
    private volatile boolean dirtyIsolation;

    private ConnectionProxy(JdbcPool pool, PooledEntry entry) {
        this.pool = pool;
        this.entry = entry;
        this.raw = entry.raw;
    }

    static Connection create(JdbcPool pool, PooledEntry entry) {
        return (Connection) Proxy.newProxyInstance(ConnectionProxy.class.getClassLoader(),
                new Class<?>[]{Connection.class}, new ConnectionProxy(pool, entry));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String name = method.getName();
        if (method.getDeclaringClass() == Object.class) {
            return switch (name) {
                case "equals" -> proxy == args[0];
                case "hashCode" -> System.identityHashCode(proxy);
                default -> "PooledConnection[" + raw + "]";
            };
        }
        switch (name) {
            case "close":
                closeConnection();
                return null;
            case "isClosed":
                return closed.get();
            case "unwrap": {
                Class<?> iface = (Class<?>) args[0];
                return iface.isInstance(proxy) ? proxy : raw.unwrap(iface);
            }
            case "isWrapperFor": {
                Class<?> iface = (Class<?>) args[0];
                return iface.isInstance(proxy) || raw.isWrapperFor(iface);
            }
            default:
                break;
        }
        if (closed.get()) {
            throw new SQLException("Connection is closed: it was returned to the pool");
        }
        switch (name) {
            case "setAutoCommit" -> dirtyAutoCommit = true;
            case "setReadOnly" -> dirtyReadOnly = true;
            case "setTransactionIsolation" -> dirtyIsolation = true;
            default -> { }
        }
        try {
            Object result = method.invoke(raw, args);
            if (result instanceof Statement statement
                    && (name.startsWith("create") || name.startsWith("prepare"))) {
                return track(statement, method.getReturnType(), proxy);
            }
            return result;
        } catch (InvocationTargetException e) {
            noteFailure(e.getCause());
            throw e.getCause();
        }
    }

    private Object track(Statement rawStatement, Class<?> type, Object connectionProxy) {
        openStatements.add(rawStatement);
        InvocationHandler handler = (p, m, a) -> {
            if (m.getDeclaringClass() == Object.class) {
                return switch (m.getName()) {
                    case "equals" -> p == a[0];
                    case "hashCode" -> System.identityHashCode(p);
                    default -> "PooledStatement[" + rawStatement + "]";
                };
            }
            if (m.getName().equals("getConnection") && m.getParameterCount() == 0) {
                return connectionProxy;
            }
            try {
                return m.invoke(rawStatement, a);
            } catch (InvocationTargetException e) {
                noteFailure(e.getCause());
                throw e.getCause();
            } finally {
                if (m.getName().equals("close")) {
                    openStatements.remove(rawStatement);
                }
            }
        };
        return Proxy.newProxyInstance(ConnectionProxy.class.getClassLoader(), new Class<?>[]{type}, handler);
    }

    /** A connection-class failure means the physical connection should not be reused. */
    private void noteFailure(Throwable t) {
        if (t instanceof SQLException e) {
            String state = e.getSQLState();
            if ((state != null && state.startsWith("08")) || e instanceof SQLNonTransientConnectionException) {
                entry.broken = true;
            }
        }
    }

    private void closeConnection() {
        if (!closed.compareAndSet(false, true)) {
            return; // second close is a no-op
        }
        for (Statement s : openStatements) {
            try {
                s.close();
            } catch (SQLException | RuntimeException ignored) {
                // a statement that cannot be closed is not a reason to lose the connection by itself
            }
        }
        openStatements.clear();
        boolean broken = entry.broken || !resetState();
        pool.release(entry, broken);
    }

    /** Returns false if the connection could not be put back into its original state. */
    private boolean resetState() {
        try {
            if (!raw.getAutoCommit()) {
                raw.rollback(); // must come before setAutoCommit(true), which would commit
            }
            if (dirtyAutoCommit) {
                raw.setAutoCommit(entry.defaultAutoCommit);
            }
            if (dirtyReadOnly) {
                raw.setReadOnly(entry.defaultReadOnly);
            }
            if (dirtyIsolation) {
                raw.setTransactionIsolation(entry.defaultIsolation);
            }
            raw.clearWarnings();
            return true;
        } catch (SQLException | RuntimeException e) {
            return false;
        }
    }
}
