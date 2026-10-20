package com.github.sup3rmar10.jdbcpool;

import java.sql.Connection;
import java.sql.SQLException;

/** Opens one new physical connection. The pool decides when. */
@FunctionalInterface
public interface ConnectionFactory {
    Connection open() throws SQLException;
}
