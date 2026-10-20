#!/usr/bin/env python
"""Hand-rolled mutation check: apply one textual mutation at a time, run the unit tests,
record whether at least one test failed, then restore the original file.

Usage:  python scripts/mutation_check.py            (all mutations)
        python scripts/mutation_check.py M05 M10    (selected ids)
Set MVN to the Maven executable if `mvn` is not on PATH.

Rules:
- Each mutation replaces `old` with `new`; `old` must occur EXACTLY ONCE in the file, otherwise the mutation is
  reported as BROKEN (the script is out of date or ambiguous), never as a survivor.
- Every test has a 10 s timeout (src/test/resources/junit-platform.properties). A mutation that makes a test hang
  therefore fails that test and counts as killed; the whole Maven run also has a 600 s ceiling (reported as
  KILLED (timeout)). A hang is not a precise detection, so the README says so.
- Files are always restored from the original bytes in a finally block, even on Ctrl-C.
"""
import os
import re
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src", "main", "java", "com", "github", "sup3rmar10", "jdbcpool")
MVN = os.environ.get("MVN", "mvn")

POOL = "JdbcPool.java"
PROXY = "ConnectionProxy.java"

MUTATIONS = [
    ("M01", "no direct handoff to the longest waiter on return", POOL,
     "if (w != null) {\n            w.entry = entry;",
     "if (w != null && false) {\n            w.entry = entry;"),
    ("M02", "waiters queued LIFO instead of FIFO", POOL,
     "waiters.addLast(w);", "waiters.addFirst(w);"),
    ("M03", "new arrivals may open a connection even when threads are waiting", POOL,
     "total < cfg.maxSize() && waiters.isEmpty()", "total < cfg.maxSize()"),
    ("M04", "max size off by one (opens max+1)", POOL,
     "total < cfg.maxSize() && waiters.isEmpty()", "total <= cfg.maxSize() && waiters.isEmpty()"),
    ("M05", "borrow timeouts are not counted", POOL,
     "timeouts.incrementAndGet();", "timeouts.get();"),
    ("M06", "validation on borrow never runs", POOL,
     "cfg.validateOnBorrow() && !isAlive(entry.raw)", "false && !isAlive(entry.raw)"),
    ("M07", "isValid is called with timeout 0 instead of the configured one", POOL,
     "c.isValid(cfg.validationTimeoutSeconds())", "c.isValid(0)"),
    ("M08", "invalid connection is replaced but the old one is not closed", POOL,
     "closeQuietly(entry.raw);\n            evicted.incrementAndGet();\n            entry = openReserved();",
     "evicted.incrementAndGet();\n            entry = openReserved();"),
    ("M09", "idle eviction ignores minSize", POOL,
     "&& total - toEvict.size() > cfg.minSize()", "&& total - toEvict.size() > 0"),
    ("M10", "max lifetime not checked when a connection is returned", POOL,
     "} else if (broken || entry.isExpired(now)) {", "} else if (broken) {"),
    ("M11", "idle connections past max lifetime are not evicted by housekeeping", POOL,
     "boolean expired = e.isExpired(now);", "boolean expired = false;"),
    ("M12", "jitter is added to the lifetime instead of subtracted", POOL,
     "expiresAt = now + cfg.maxLifetimeMillis() - jitter;", "expiresAt = now + cfg.maxLifetimeMillis() + jitter;"),
    ("M13", "a leak is reported on every housekeeping pass", POOL,
     "e.leakReported = true;", "e.leakReported = false;"),
    ("M14", "leak threshold comparison is strict (> instead of >=)", POOL,
     "held >= cfg.leakThresholdMillis()", "held > cfg.leakThresholdMillis()"),
    ("M15", "keepalive interval comparison is strict (> instead of >=)", POOL,
     "now - e.lastAccessed >= cfg.keepaliveMillis()", "now - e.lastAccessed > cfg.keepaliveMillis()"),
    ("M16", "a successful keepalive ping does not count as activity", POOL,
     "entry.lastAccessed = clock.millis();\n                returnToIdleLocked(entry);",
     "returnToIdleLocked(entry);"),
    ("M17", "shutdown does not fail waiting borrowers", POOL,
     "w.failure = new PoolClosedException(\"The pool is shutting down\");", "w.failure = null;"),
    ("M18", "shutdown does not wait for lent connections", POOL,
     "while (total > 0 && remaining > 0) {", "while (total > 0 && remaining > 0 && false) {"),
    ("M19", "connections still lent after the deadline are not force-closed", POOL,
     "if (detachLocked(e)) {\n                        forced.add(e);", "if (e == null) {\n                        forced.add(e);"),
    ("M20", "uncommitted work is not rolled back on return", PROXY,
     "raw.rollback(); // must come before setAutoCommit(true), which would commit", "raw.getAutoCommit();"),
    ("M21", "double close is not guarded", PROXY,
     "if (!closed.compareAndSet(false, true)) {", "if (!closed.compareAndSet(false, true) && false) {"),
    ("M22", "open statements are not closed on return", PROXY,
     "s.close();", "s.isClosed();"),
    ("M23", "read-only flag is not restored on return", PROXY,
     "raw.setReadOnly(entry.defaultReadOnly);", "raw.isReadOnly();"),
    ("M24", "warnings are not cleared on return", PROXY,
     "raw.clearWarnings();", "raw.getAutoCommit();"),
    ("M25", "connection-class SQLState (08xxx) no longer marks the connection broken", PROXY,
     "state.startsWith(\"08\")", "state.startsWith(\"99\")"),
]


def run_tests():
    try:
        p = subprocess.run([MVN, "-B", "-q", "test"], cwd=ROOT, capture_output=True, text=True, timeout=600)
    except subprocess.TimeoutExpired:
        return "timeout", []
    out = p.stdout + p.stderr
    if "COMPILATION ERROR" in out:
        return "compile-error", []
    failing = sorted(set(re.findall(r"\[ERROR\]\s{2,}(\w+Test\.\w+)", out)))
    return ("killed" if p.returncode != 0 else "survived"), failing


def main():
    wanted = set(sys.argv[1:])
    originals = {}
    results = []
    try:
        for mid, desc, fname, old, new in MUTATIONS:
            if wanted and mid not in wanted:
                continue
            path = os.path.join(SRC, fname)
            if fname not in originals:
                with open(path, "rb") as f:
                    originals[fname] = f.read()
            text = originals[fname].decode("utf-8").replace("\r\n", "\n")
            occurrences = text.count(old)
            if occurrences != 1:
                results.append((mid, desc, "BROKEN (pattern found %d times, need exactly 1)" % occurrences, []))
                print(f"{mid} {desc}: BROKEN ({occurrences} matches)", flush=True)
                continue
            with open(path, "w", encoding="utf-8", newline="\n") as f:
                f.write(text.replace(old, new, 1))
            try:
                status, failing = run_tests()
            finally:
                with open(path, "wb") as f:
                    f.write(originals[fname])
            label = {"killed": "KILLED", "survived": "SURVIVED", "compile-error": "COMPILE ERROR (invalid)",
                     "timeout": "KILLED (timeout)"}[status]
            results.append((mid, desc, label, failing))
            print(f"{mid} | {desc} | {label} | {len(failing)} failing: {', '.join(failing[:3])}", flush=True)
    finally:
        for fname, data in originals.items():
            with open(os.path.join(SRC, fname), "wb") as f:
                f.write(data)
    killed = sum(1 for r in results if r[2].startswith("KILLED"))
    print(f"\n{killed}/{len(results)} mutations killed")


if __name__ == "__main__":
    main()
