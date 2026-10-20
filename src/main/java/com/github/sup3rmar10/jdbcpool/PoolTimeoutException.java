package com.github.sup3rmar10.jdbcpool;

import java.sql.SQLTransientConnectionException;

/** No connection became available within the borrow timeout. */
public class PoolTimeoutException extends SQLTransientConnectionException {
    private static final long serialVersionUID = 1L;

    public PoolTimeoutException(String reason) {
        super(reason, "08001");
    }
}
