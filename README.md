# java-jdbc-pool

A small JDBC connection pool in Java 17 (a `javax.sql.DataSource`), written to understand what HikariCP-style pools
actually do: bounded size, fair waiting, validation, eviction, leak detection, state reset on return, and shutdown.
**It is not a replacement for HikariCP and has not been benchmarked against it.** The only dependencies are H2
(runtime, for the Demo and integration tests) and JUnit 5 (test).

What it does:

- `minSize` / `maxSize`; connections are opened lazily up to max, and topped up to min by housekeeping.
- Borrow timeout with **fair FIFO waiters**: a returned connection is handed directly to the longest waiter; a newly
  arriving thread cannot overtake threads already waiting. Timeout throws `PoolTimeoutException`.
- **Idle eviction** (only above `minSize`), **max lifetime with jitter** (each connection lives `maxLifetime - rand[0, jitter]`,
  default jitter 2.5%), **validation on borrow** (`Connection.isValid(timeoutSeconds)`; a bad connection is replaced
  in place) and an optional **keepalive** ping of idle connections.
- **Leak detection**: a stack trace is captured at borrow time; housekeeping reports connections held longer than the
  threshold once, through a listener.
- A **proxy connection** per borrow: `close()` rolls back uncommitted work, restores auto-commit / read-only /
  transaction isolation (only the ones the caller changed), clears warnings, closes statements that are still open,
  and returns the connection instead of closing it. Double close is a no-op; any other call after close throws.
  A failed reset, or an SQLException with SQLState `08xxx` (or a `SQLNonTransientConnectionException`), marks the
  physical connection broken so it is discarded instead of reused.
- **Shutdown** that fails waiting borrowers, closes idle connections, waits for lent connections up to a deadline,
  then closes whatever is still lent and reports `false`.
- **Metrics snapshot**: active, idle, waiting, total, timeouts, created, evicted (plus leaks, validation failures, create failures).
- Injected `Clock` and `Scheduler`, so eviction, lifetime, keepalive and leak behaviour is tested without sleeping.

## How to run

```bash
mvn -q -B verify                       # compile, run the tests, copy h2 to target/lib

# ':' is the classpath separator on Linux/macOS/Git Bash; use ';' in Windows cmd/PowerShell.
java -cp "target/classes:target/lib/*" com.github.sup3rmar10.jdbcpool.Demo
```

Usage sketch:

```java
PoolConfig cfg = PoolConfig.builder("jdbc:h2:mem:x", "sa", "")
        .minSize(2).maxSize(10).borrowTimeoutMillis(2_000)
        .maxLifetimeMillis(30 * 60_000).idleTimeoutMillis(10 * 60_000)
        .leakThresholdMillis(15_000).keepaliveMillis(60_000).build();
try (JdbcPool pool = new JdbcPool(cfg)) {
    try (Connection c = pool.getConnection()) { /* use it, then close() returns it */ }
    System.out.println(pool.metrics());
}
```

## Demo output (one real run)

The Demo uses the real clock and real threads, so numbers differ per run.

```
== 1. contention: 12 threads, 3 connections
queries run: 60, most connections held at once: 3 (max 3), most threads waiting at once (sampled): 9
metrics: PoolMetrics[active=0, idle=3, waiting=0, total=3, timeouts=0, created=3, evicted=0, leaksDetected=0, validationFailures=0, createFailures=0]
== 2. leak report: borrow, forget to close
leak report received after >= 100 ms; borrowing thread 'main', first stack lines:
  java.lang.Throwable: connection borrowed here
  at com.github.sup3rmar10.jdbcpool.JdbcPool.getConnection(JdbcPool.java:89)
  at com.github.sup3rmar10.jdbcpool.Demo.leakyMethod(Demo.java:110)
  at com.github.sup3rmar10.jdbcpool.Demo.leak(Demo.java:94)
metrics after returning it: PoolMetrics[active=0, idle=1, waiting=0, total=1, timeouts=0, created=1, evicted=0, leaksDetected=1, validationFailures=0, createFailures=0]
== 3. poisoned connection replaced
first borrow ok, physical connections opened so far: 1
second borrow ok, SELECT 42 -> 42
physical connections opened: 2, validationFailures=1, evicted=1, created=2
== 4. crude single-thread timing (one run, in-memory H2, NOT a benchmark, no warm-up control)
pool borrow+return: 123379 per second; DriverManager open+close: 1095 per second
(Says nothing about HikariCP: it was not measured. Opening an in-memory H2 connection involves no network.)
```

The throughput line in section 4 is a crude single-thread, single-run timing of "borrow and return" against
"open and close a new connection" on in-memory H2. It is **not** a benchmark, says nothing about HikariCP (which I did
not measure), and must not be read as a performance claim.

## Design notes

- **One lock, nothing slow inside it.** All pool state sits behind one `ReentrantLock`. Opening, validating, pinging and
  closing physical connections, and calling the leak listener, all happen outside it. This is simpler to reason about than a
  lock-free design, and a single lock is also the obvious contention point under very high borrow rates.
- **Accounting rule.** `total` counts every connection the pool is responsible for, including ones being opened,
  validated or pinged, and is decreased only *after* the physical connection is closed. That makes "never more than
  maxSize open physical connections" true, not just "never more than maxSize lent". The price is that a slow `close()`
  holds its slot a little longer.
- **Fairness by handoff.** `release` gives the connection straight to the head of the waiter queue instead of putting it
  in the idle queue and waking someone, so a thread that arrives a microsecond later cannot steal it. A freed *slot*
  (connection closed while threads wait) is likewise handed to the head waiter as a permission to open one.
- **Validation failure replaces in place.** The bad connection is closed and a new one opened in the same slot, so a
  waiter who was handed a dead connection does not go to the back of the line.
- **Idle queue is LIFO.** The most recently used connection is lent first, so surplus connections at the other end age out.
- **Lifetime of a lent connection.** Max lifetime never closes a connection that is in use; it is checked when the
  connection comes back, and expired idle connections are closed by housekeeping.
- **Leak detection is a periodic scan**, not a per-borrow timer like HikariCP's. So the report arrives at the first
  housekeeping pass after the threshold, not exactly at the threshold.
- **Time.** Ages (idle, lifetime, keepalive, leak) use the injected `Clock`; the borrow timeout uses `System.nanoTime()`
  because a waiting thread has to be woken for real. Timeout tests therefore use real (short) timeouts in a situation
  where nobody can return a connection, so the outcome is certain.
- **A proxy per borrow** (`java.lang.reflect.Proxy`), so a stale reference held after `close()` can never reach a
  connection that has since been lent to somebody else.

## Tests

`mvn test` runs 103 JUnit tests. Time-based behaviour uses a manual clock and a manual scheduler; the
fake connection used by most tests records calls, so reset order (rollback before setAutoCommit) and `isValid` timeouts
are asserted directly. Where a test needs another thread to be in a particular state (queued as a waiter, blocked in
shutdown) it polls a condition with a 10 s ceiling instead of sleeping for a fixed time. The concurrency tests
(`StressTest`) run many threads against a small pool and assert: open physical connections never exceed max, every
borrow ends in exactly one of "got a connection" or "timed out", no connection is lost
(`created - evicted == total` once idle), and the pool's own metrics agree with what the callers saw. They are
randomised by thread scheduling, so a pass is evidence, not proof.

## Mutation check

Passing tests prove little if they would also pass on broken code, so I applied 25 behaviour-defining mutations one at a
time (textual single replacements run by `scripts/mutation_check.py`, which requires each match text to occur exactly once
and restores each file afterwards in a `finally` block), ran `mvn test`, and recorded whether any test failed. Every test
has a 10 s timeout (`src/test/resources/junit-platform.properties`), so a mutation that makes something hang fails that test and
counts as a kill; a hang is a blunt detector, not a precise one. This is a hand-rolled check, not PIT, and the mutations are
the ones I thought of, so it says nothing about bugs I did not think of.

**Result: 24 of 25 killed, 1 survived** (M03, explained below). The first full run killed 22: M03 and M17 survived and M22 was
reported as an invalid mutant (compile error). The final number comes from fixing the M22 mutation, adding an assertion for
M17, and re-running M17, M22 and M03 individually. The other 22 results are from the first full run; between the two runs the
only changes were that one test edit, the M22 mutation text, and the wording of one Demo line.

| # | Mutation | Result | First failing test(s) |
|---|---|---|---|
| M01 | no direct handoff to the longest waiter on return | killed | `BorrowTest.aNewArrivalCannotOvertakeAWaiter`, 6 more |
| M02 | waiters queued LIFO instead of FIFO | killed | `BorrowTest.waitersAreServedInArrivalOrder`, 1 more |
| M03 | new arrivals may open a connection even when threads are waiting | **survived** | none; see notes |
| M04 | max size off by one (opens max+1) | killed | `BorrowTest.aTimedOutBorrowDoesNotLoseOrLeakASlot`, 16 more |
| M05 | borrow timeouts not counted | killed | `BorrowTest.timeoutIsCountedAndTheMessageSaysWhy`, 1 more |
| M06 | validation on borrow never runs | killed | `ValidationTest`, `H2IntegrationTest` cases, 8 failing |
| M07 | isValid called with timeout 0, not the configured one | killed | `ValidationTest.anIdleConnectionIsValidatedWithTheConfiguredTimeout`, 1 more |
| M08 | invalid connection replaced but old one not closed | killed | `ValidationTest.anInvalidIdleConnectionIsClosedAndReplaced`, 3 more |
| M09 | idle eviction ignores minSize | killed | `HousekeepingTest.idleEvictionNeverGoesBelowMin`, 1 more |
| M10 | max lifetime not checked on return | killed | `HousekeepingTest.aLentConnectionPastItsLifetime...` (only this one) |
| M11 | idle connections past max lifetime not evicted | killed | `HousekeepingTest.anIdleConnectionIsClosedAtMaxLifetime...`, 3 more |
| M12 | jitter added instead of subtracted | killed | `HousekeepingTest.jitterShortensTheLifetime...`, 1 more |
| M13 | leak reported on every housekeeping pass | killed | `HousekeepingTest.aLeakIsReportedOnlyOnce`, 2 more |
| M14 | leak threshold comparison strict (`>` not `>=`) | killed | `HousekeepingTest.aLeakIsReportedWithTheStack...`, 2 more |
| M15 | keepalive interval comparison strict | killed | `HousekeepingTest.keepalivePingsIdleConnectionsAtTheConfiguredInterval`, 1 more |
| M16 | successful ping does not count as activity | killed | `HousekeepingTest.keepalivePingsIdleConnectionsAtTheConfiguredInterval` (only this one) |
| M17 | shutdown does not fail waiting borrowers | **survived, then killed** after a test fix (see notes) | `ShutdownTest.waitingBorrowersFailWithPoolClosedWhenShutdownStarts` |
| M18 | shutdown does not wait for lent connections | killed | `ShutdownTest.shutdownWaitsForALentConnectionToComeBack`, 1 more |
| M19 | lent connections not force-closed after the deadline | killed | `ShutdownTest.afterTheDeadlineLentConnections...`, 1 more |
| M20 | uncommitted work not rolled back on return | killed | `ConnectionProxyTest.aFailedResetDiscardsThePhysicalConnection`, 6 more |
| M21 | double close not guarded | killed | `ConnectionProxyTest.doubleCloseDoesNotPutTheConnectionInTheIdleQueueTwice` (only this one) |
| M22 | open statements not closed on return | killed (first run: invalid mutant, see notes) | `ConnectionProxyTest.openStatementsAreClosedOnReturn`, 1 more |
| M23 | read-only flag not restored | killed | `ConnectionProxyTest.readOnlyIsRestored` (only this one) |
| M24 | warnings not cleared | killed | `ConnectionProxyTest.warningsAreCleared` (only this one) |
| M25 | SQLState 08xxx no longer marks connection broken | killed | `ConnectionProxyTest.aConnectionClassSqlStateMarksTheConnectionBroken` (only this one) |

Notes on the results:

- **M17 was a real test gap, now closed.** With the mutation, `shutdown` woke waiting borrowers without telling them the pool
  was closed. They then looped until their own borrow timeout expired, found themselves fulfilled by nobody, fell through to
  "open a connection", and only then failed with `PoolClosedException` (via `lend`). The old test used a 5 s borrow timeout and
  still passed because the final exception was the same, just late. The test now uses a 120 s borrow timeout, so a waiter
  that is not failed by shutdown itself makes the test hit its 10 s limit. Re-run: M17 killed.
- **M22 first showed as a COMPILE ERROR**: my replacement (`s.hashCode();`) made the surrounding `catch (SQLException ...)`
  unreachable, so the mutant was invalid, not a survivor or a kill. I changed it to `s.isClosed();` and re-ran: killed.
- **M03 survived and I believe it is an equivalent mutant, but I have not proven it.** The `waiters.isEmpty()` check in the
  "may I open a new connection" branch is defensive: whenever a slot is freed while threads are waiting, `slotFreedLocked` hands
  it to the head waiter under the same lock hold, so "waiters present and total below max" should never be observable. If that
  reasoning is right, no test can distinguish the two versions. I kept the check as a guard against a future code path that frees a
  slot without calling `slotFreedLocked`.
- M10, M16, M21, M23, M24 and M25 are each killed by exactly one test, so those behaviours are covered thinly.

Reproduce: `python scripts/mutation_check.py` (set `MVN` if `mvn` is not on PATH; pass ids such as `M17 M22` to run a subset).
A full run took roughly 25 to 30 minutes on my Windows machine (about 1 minute per mutation, most of it Maven start-up).

## Known limits / not verified

- Not benchmarked, and not compared with HikariCP in any way. HikariCP has years of production hardening, a
  lock-free `ConcurrentBag`, per-thread caching and many more features (metrics integrations, JMX, `connectionInitSql`,
  `initializationFailTimeout`, catalog/schema reset, `maxLifetime` per-connection timers).
- Only H2 (2.3.232, in-memory) was used for real JDBC. No PostgreSQL, MySQL, Oracle or SQL Server run; behaviour of
  `isValid`, `rollback` and isolation resets on those drivers is assumed from the JDBC contract, not observed.
- Catalog, schema, network timeout, type map and client info are **not** reset on return. `DatabaseMetaData.getConnection()`
  and `ResultSet.getStatement()` return the raw connection/statement, which bypasses the proxy.
- Statement tracking wraps `createStatement`, `prepareStatement` and `prepareCall`; there is no prepared-statement cache.
- `getConnection(user, password)`, `setLoginTimeout` and `getParentLogger` are not supported.
- The initial fill to `minSize` is best effort: a failure is counted (`createFailures`) and the pool starts anyway
  (no fail-fast option). If the database is down, the next borrow reports the real error.
- Leak detection reports only; it never closes a leaked connection. Keepalive pings run one after another on the
  housekeeping thread, so many slow pings delay the other housekeeping work.
- Shutdown force-closes connections still lent after the deadline; their holders see driver errors on next use.
- Under extreme contention the single lock will be the limit; I did not measure where.
- CI: the workflow is included. CI ran green on the first run on GitHub Actions (Java 17 and 21, ubuntu-latest, 2026-10-20).

## Layout

```
src/main/java/com/github/sup3rmar10/jdbcpool/   JdbcPool, ConnectionProxy, PoolConfig, PoolMetrics, Clock, Scheduler, Demo, ...
src/test/java/...                               unit tests on a fake JDBC layer, H2 integration tests, stress tests
scripts/mutation_check.py                       the mutation check described above
```

Clone: `git clone https://github.com/Supp3r-Man/java-jdbc-pool.git`

MIT licensed.
