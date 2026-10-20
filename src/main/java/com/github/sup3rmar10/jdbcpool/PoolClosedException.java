package com.github.sup3rmar10.jdbcpool;

import java.sql.SQLNonTransientConnectionException;

/** The pool has been shut down. */
public class PoolClosedException extends SQLNonTransientConnectionException {
    private static final long serialVersionUID = 1L;

    public PoolClosedException(String reason) {
        super(reason, "08003");
    }
}
