package com.github.sup3rmar10.jdbcpool;

import java.io.PrintWriter;
import java.io.StringWriter;

/** A connection that has been borrowed for longer than the leak threshold. */
public record LeakReport(String threadName, long heldMillis, Throwable borrowedAt) {
    /** Human readable text including the stack captured when the connection was borrowed. */
    public String describe() {
        StringWriter sw = new StringWriter();
        borrowedAt.printStackTrace(new PrintWriter(sw));
        return "Connection leak suspected: borrowed by thread '" + threadName + "' and not returned after "
                + heldMillis + " ms\n" + sw;
    }
}
