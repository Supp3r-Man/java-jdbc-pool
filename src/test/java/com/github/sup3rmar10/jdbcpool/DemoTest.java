package com.github.sup3rmar10.jdbcpool;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class DemoTest {
    @Test
    void theDemoRunsAndShowsContentionALeakAndAReplacedConnection() throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        Demo.run(new PrintStream(buffer, true, StandardCharsets.UTF_8));
        String out = buffer.toString(StandardCharsets.UTF_8);
        assertTrue(out.contains("queries run: 60, "), out);
        assertTrue(java.util.regex.Pattern.compile("most connections held at once: [1-3] \\(max 3\\)").matcher(out).find(), out);
        assertTrue(out.contains("leaksDetected=1"), out);
        assertTrue(out.contains("leak report received"), out);
        assertTrue(out.contains("validationFailures=1"), out);
        assertTrue(out.contains("SELECT 42 -> 42"), out);
        assertTrue(out.contains("NOT a benchmark"), out);
    }
}
