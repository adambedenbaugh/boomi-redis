package com.boomi.connector.redis.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.listen.Listener;
import com.boomi.connector.api.listen.ListenerExecutionResult;
import com.boomi.connector.api.listen.ListenerStatus;
import com.boomi.connector.api.listen.SubmitOptions;
import com.boomi.connector.redis.RedisConnection;
import com.boomi.connector.redis.pool.RedisListenManager;
import org.junit.Test;
import org.mockito.MockedConstruction;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RedisListenOperationTest {

    private OperationContext contextWithQueueKey(String queueKey, boolean singleton) {
        PropertyMap opProps = mock(PropertyMap.class);
        when(opProps.getProperty("queue_type", "List")).thenReturn("List");
        when(opProps.getProperty("queue_key", "")).thenReturn(queueKey);
        when(opProps.getBooleanProperty("singleton_mode", Boolean.FALSE)).thenReturn(singleton);
        when(opProps.getLongProperty("poll_block_ms", 5000L)).thenReturn(1000L);
        OperationContext ctx = mock(OperationContext.class);
        when(ctx.getOperationProperties()).thenReturn(opProps);
        return ctx;
    }

    @Test(expected = ConnectorException.class)
    public void emptyQueueKeyThrowsAtConstruction() {
        new RedisListenOperation(contextWithQueueKey("", false));
    }

    @Test
    public void isSingletonReflectsFieldWhenTrue() {
        RedisListenOperation op = new RedisListenOperation(contextWithQueueKey("q", true));
        assertTrue(op.isSingleton());
    }

    @Test
    public void isSingletonReflectsFieldWhenFalse() {
        RedisListenOperation op = new RedisListenOperation(contextWithQueueKey("q", false));
        assertFalse(op.isSingleton());
    }

    @Test
    public void startReturnsPromptlyAndStopJoinsTheThread() throws Exception {
        RedisListenOperation op = new RedisListenOperation(contextWithQueueKey("q", false));
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);
        RedisListenManager manager = new RedisListenManager();

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, context) -> when(mock.blpop(eq("q"), anyInt())).thenReturn(null))) {

            long startedAt = System.currentTimeMillis();
            op.start(listener, manager);
            assertTrue("start() must return promptly", System.currentTimeMillis() - startedAt < 2000);

            op.stop();
            verify(mocked.constructed().get(0)).close();
        }
    }

    @Test(timeout = 20000)
    public void stopJoinTimeoutScalesWithConfiguredPollBlockDuration() throws Exception {
        // poll_block_ms = 10s makes the NEW dynamic formula's timeout
        // max(10_000, 10_000 + 5_000) = 15_000ms. The simulated in-flight blpop() below blocks for
        // ~11.5s -- longer than the OLD hardcoded 10s join() timeout, but comfortably inside the
        // new ~15s one. That is the only window that actually discriminates the two behaviors:
        // with the OLD hardcoded 10_000L join(), pollThread.join(10_000) returns (timed out, thread
        // still alive/sleeping) at ~10s elapsed and stop() proceeds immediately; with the NEW
        // dynamic ~15s timeout, join() blocks until the poll thread genuinely terminates at
        // ~11.5s elapsed. Wall-clock time of the stop() call is therefore the discriminator: this
        // test asserts stop() took at least 11s, which only the fixed code satisfies.
        PropertyMap opProps = mock(PropertyMap.class);
        when(opProps.getProperty("queue_type", "List")).thenReturn("List");
        when(opProps.getProperty("queue_key", "")).thenReturn("q");
        when(opProps.getBooleanProperty("singleton_mode", Boolean.FALSE)).thenReturn(false);
        when(opProps.getLongProperty("poll_block_ms", 5000L)).thenReturn(10000L);
        OperationContext ctx = mock(OperationContext.class);
        when(ctx.getOperationProperties()).thenReturn(opProps);

        RedisListenOperation op = new RedisListenOperation(ctx);
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);
        RedisListenManager manager = new RedisListenManager();

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, context) -> when(mock.blpop(eq("q"), anyInt())).thenAnswer(invocation -> {
                    // A real Jedis blpop() reads from a plain socket and is NOT responsive to
                    // Thread.interrupt() -- so this simulated "blocking call" must ignore
                    // interrupts too, otherwise stop()'s (harmless) interrupt() call would
                    // shorten the wait and the test would stop proving anything about the join
                    // timeout math. This is a REAL sleep -- do not mock Thread.sleep -- because the
                    // whole point is to prove the join() timeout genuinely outlasts a real 11.5s
                    // in-flight call.
                    long deadline = System.currentTimeMillis() + 11_500L;
                    while (System.currentTimeMillis() < deadline) {
                        try {
                            Thread.sleep(deadline - System.currentTimeMillis());
                        } catch (InterruptedException ignored) {
                            // swallow, matching an uninterruptible blocking socket read
                        }
                    }
                    return null;
                }))) {

            op.start(listener, manager);
            // Invoke stop() shortly after start(), while the poll thread is genuinely still
            // inside its simulated "blocking" blpop() call.
            Thread.sleep(200L);

            long stopStartedAt = System.currentTimeMillis();
            op.stop();
            long stopElapsedMs = System.currentTimeMillis() - stopStartedAt;

            verify(mocked.constructed().get(0), times(1)).close();
            assertTrue("stop() must genuinely wait out the ~11.5s in-flight blpop() call via "
                    + "join() rather than timing out early and racing ahead to close() -- elapsed="
                    + stopElapsedMs + "ms (expected >= 11000ms)", stopElapsedMs >= 11_000L);
        }
    }

    @Test
    public void connectionFailureReportsOfflineThenOnlineOnRecovery() throws Exception {
        RedisListenOperation op = new RedisListenOperation(contextWithQueueKey("q", false));
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class, (mock, context) ->
                when(mock.blpop(eq("q"), anyInt()))
                        .thenThrow(new RuntimeException("connection reset"))
                        .thenReturn(null))) {
            op.start(listener, new RedisListenManager());
            // backoff() sleeps a real, uninterruptible-by-design 5000ms after a failed poll before
            // the loop retries. stop() sets `stopping` before calling interrupt(), so if stop() is
            // invoked while that sleep is still in flight, the interrupted backoff() returns
            // straight into a loop-top check that immediately exits (stopping is already true) --
            // the recovery (second, successful) poll, and thus the ONLINE update, would never run.
            // Wait out the backoff naturally (with margin) so the successful retry genuinely
            // happens before stop() is called -- matching this class's established pattern of real
            // sleeps proving real timing rather than mocked/instant ones.
            Thread.sleep(5500);
            op.stop();

            verify(listener, atLeastOnce()).updateStatus(eq(ListenerStatus.OFFLINE), anyString());
            verify(listener, atLeastOnce()).updateStatus(ListenerStatus.ONLINE);
        }
    }

    @Test
    public void shouldLogFailureAlwaysLogsTheFirstOccurrence() {
        assertTrue(RedisListenOperation.shouldLogFailure(1, 1_000L, 0L));
    }

    @Test
    public void shouldLogFailureThrottlesARepeatWithinTheWindow() {
        long now = 1_000L;
        long lastLoggedAt = now - (RedisListenOperation.FAILURE_LOG_THROTTLE_MS / 2);
        assertFalse(RedisListenOperation.shouldLogFailure(2, now, lastLoggedAt));
    }

    @Test
    public void shouldLogFailureLogsAgainOnceTheThrottleWindowElapses() {
        long lastLoggedAt = 1_000L;
        long now = lastLoggedAt + RedisListenOperation.FAILURE_LOG_THROTTLE_MS;
        assertTrue(RedisListenOperation.shouldLogFailure(50, now, lastLoggedAt));
    }

    @Test
    public void connectionExceptionClosesTheBrokenConnectionAndAttemptsToReconnect() throws Exception {
        RedisListenOperation op = new RedisListenOperation(contextWithQueueKey("q", false));
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, context) -> when(mock.blpop(eq("q"), anyInt()))
                        .thenThrow(new JedisConnectionException("Broken pipe (Write failed)")))) {
            op.start(listener, new RedisListenManager());
            // Enough time for the first poll iteration to run and fail.
            Thread.sleep(300);
            op.stop();

            // The broken connection must never be reused -- that was the root cause of the
            // production incident this fixes: Jedis never repairs a broken Connection internally,
            // so retrying the SAME instance forever meant the listener stayed dead until a human
            // manually stopped/started it. reconnectQuietly()'s own replacement attempt runs on the
            // poll thread it spawns; Mockito's construction mocking is scoped to the thread that
            // opened it and cannot observe a construction made from a different thread, so a real
            // (unmocked) replacement is constructed here -- proving the *attempt* happens is covered
            // end-to-end, against real Redis, by
            // RedisListenSendIT.listenerReconnectsAfterItsConnectionIsForciblyKilled.
            verify(mocked.constructed().get(0)).close();
        }
    }

    @Test
    public void repeatedConnectionFailuresWithinTheThrottleWindowAreNotAllLogged() throws Exception {
        RedisListenOperation op = new RedisListenOperation(contextWithQueueKey("q", false));
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);

        Logger julLogger = Logger.getLogger(RedisListenOperation.class.getName());
        List<LogRecord> severeRecords = new CopyOnWriteArrayList<>();
        Handler captor = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getLevel() == Level.SEVERE) {
                    severeRecords.add(record);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        julLogger.addHandler(captor);

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, context) -> when(mock.blpop(eq("q"), anyInt()))
                        .thenThrow(new JedisConnectionException("Broken pipe (Write failed)")))) {
            op.start(listener, new RedisListenManager());
            // Two backoff cycles (5s each, real) + margin: iteration 1 fails on the original
            // (mocked) connection and is logged; the reconnect attempt it triggers fails for real
            // (see the note in the test above -- the replacement connection is genuinely
            // constructed on the poll thread, unmocked, and genuinely fails to init() against this
            // test's incomplete connection properties), leaving the connection null; iteration 2
            // then fails again via the "no connection available" guard -- still within the 5-minute
            // throttle window, so it must NOT be logged again.
            Thread.sleep(11_000);
            op.stop();

            assertEquals("only the first failure in an ongoing outage should be logged while within "
                    + "the throttle window", 1, severeRecords.size());
        } finally {
            julLogger.removeHandler(captor);
        }
    }

    @Test
    public void listModePollSubmitsPoppedValueAsPayload() throws Exception {
        RedisListenOperation op = new RedisListenOperation(contextWithQueueKey("q", false));
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);
        RedisListenManager manager = new RedisListenManager();

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, context) -> when(mock.blpop(eq("q"), anyInt())).thenReturn("hello"))) {

            op.start(listener, manager);
            // Give the poll thread a moment to run at least one iteration.
            Thread.sleep(200);
            op.stop();

            verify(listener, atLeastOnce()).submit(argThat((Payload payload) -> true));
        }
    }

    private OperationContext streamContext(String ackMode) {
        PropertyMap connProps = mock(PropertyMap.class);
        PropertyMap opProps = mock(PropertyMap.class);
        when(opProps.getProperty("consumer_group_name", "")).thenReturn("mygroup");
        when(opProps.getProperty("queue_type", "List")).thenReturn("Stream");
        when(opProps.getProperty("queue_key", "")).thenReturn("s");
        when(opProps.getBooleanProperty("singleton_mode", Boolean.FALSE)).thenReturn(false);
        when(opProps.getLongProperty("poll_block_ms", 5000L)).thenReturn(1000L);
        when(opProps.getProperty("ack_mode", "OnCompletion")).thenReturn(ackMode);
        when(opProps.getLongProperty("idle_reclaim_timeout_ms", 60000L)).thenReturn(50L);
        when(opProps.getLongProperty("max_delivery_attempts", 5L)).thenReturn(3L);
        when(opProps.getProperty("dead_letter_key", "")).thenReturn("");
        OperationContext ctx = mock(OperationContext.class);
        when(ctx.getOperationProperties()).thenReturn(opProps);
        when(ctx.getConnectionProperties()).thenReturn(connProps);
        return ctx;
    }

    private OperationContext streamContext(String ackMode, long idleReclaimTimeoutMs) {
        PropertyMap connProps = mock(PropertyMap.class);
        PropertyMap opProps = mock(PropertyMap.class);
        when(opProps.getProperty("consumer_group_name", "")).thenReturn("mygroup");
        when(opProps.getProperty("queue_type", "List")).thenReturn("Stream");
        when(opProps.getProperty("queue_key", "")).thenReturn("s");
        when(opProps.getBooleanProperty("singleton_mode", Boolean.FALSE)).thenReturn(false);
        when(opProps.getLongProperty("poll_block_ms", 5000L)).thenReturn(1000L);
        when(opProps.getProperty("ack_mode", "OnCompletion")).thenReturn(ackMode);
        // RedisListenOperation.longPropertyOrDefault() always calls getLongProperty(key, DEFAULT) --
        // stub it explicitly (a bare mock does not apply the real fallback behavior to unstubbed
        // calls) so ack-on-completion's future.get(timeout, unit) bound is deterministic in tests.
        when(opProps.getLongProperty(eq("idle_reclaim_timeout_ms"), any())).thenReturn(idleReclaimTimeoutMs);
        OperationContext ctx = mock(OperationContext.class);
        when(ctx.getOperationProperties()).thenReturn(opProps);
        when(ctx.getConnectionProperties()).thenReturn(connProps);
        return ctx;
    }

    @Test(expected = ConnectorException.class)
    public void streamModeWithoutConsumerGroupThrowsAtConstruction() {
        PropertyMap connProps = mock(PropertyMap.class);
        PropertyMap opProps = mock(PropertyMap.class);
        when(opProps.getProperty("consumer_group_name", "")).thenReturn("");
        when(opProps.getProperty("queue_type", "List")).thenReturn("Stream");
        when(opProps.getProperty("queue_key", "")).thenReturn("s");
        when(opProps.getBooleanProperty("singleton_mode", Boolean.FALSE)).thenReturn(false);
        when(opProps.getLongProperty("poll_block_ms", 5000L)).thenReturn(1000L);
        OperationContext ctx = mock(OperationContext.class);
        when(ctx.getOperationProperties()).thenReturn(opProps);
        when(ctx.getConnectionProperties()).thenReturn(connProps);

        new RedisListenOperation(ctx);
    }

    @Test
    public void streamModeCreatesConsumerGroupOnStart() throws Exception {
        RedisListenOperation op = new RedisListenOperation(streamContext("OnSubmission"));
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, context) -> when(mock.xreadGroup(anyString(), anyString(), anyString(), anyInt(), anyInt()))
                        .thenReturn(java.util.Collections.emptyList()))) {
            op.start(listener, new RedisListenManager());
            op.stop();
            verify(mocked.constructed().get(0)).xgroupCreate(eq("s"), eq("mygroup"));
        }
    }

    @Test
    public void startClosesConnectionAndPropagatesExceptionWhenXgroupCreateFails() throws Exception {
        // Regression test for the Critical review finding: start() had no try/catch/finally around
        // connection setup, so a failing xgroupCreate() (e.g. Queue Key points at the wrong Redis
        // type, or Redis becomes unreachable between init() and xgroupCreate()) leaked the
        // RedisConnection -- permanently pinning a pooled reference or leaking a live socket. The
        // fix must still let the exception propagate (the SDK contract requires start() to throw on
        // failure) but must close() the connection first.
        RedisListenOperation op = new RedisListenOperation(streamContext("OnSubmission"));
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);

        RuntimeException xgroupCreateFailure = new RuntimeException("WRONGTYPE Operation against a key "
                + "holding the wrong kind of value");
        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, context) -> doThrow(xgroupCreateFailure).when(mock)
                        .xgroupCreate(anyString(), anyString()))) {
            try {
                op.start(listener, new RedisListenManager());
                fail("start() must propagate the xgroupCreate() failure");
            } catch (RuntimeException e) {
                assertSame("start() must propagate the original exception, not wrap/swallow it",
                        xgroupCreateFailure, e);
            }

            verify(mocked.constructed().get(0), times(1)).close();
        }
    }

    @Test
    public void ackOnSubmissionAcksImmediatelyAfterSubmit() throws Exception {
        RedisListenOperation op = new RedisListenOperation(streamContext("OnSubmission"));
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);

        com.boomi.connector.redis.connection.RedisStreamMessage message =
                new com.boomi.connector.redis.connection.RedisStreamMessage("1-1", "v");

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, context) -> when(mock.xreadGroup(eq("s"), eq("mygroup"), anyString(), anyInt(), anyInt()))
                        .thenReturn(java.util.Collections.singletonList(message))
                        .thenReturn(java.util.Collections.emptyList()))) {
            op.start(listener, new RedisListenManager());
            Thread.sleep(200);
            op.stop();

            verify(listener, atLeastOnce()).submit(any(com.boomi.connector.api.Payload.class));
            verify(mocked.constructed().get(0), atLeastOnce()).xack("s", "mygroup", "1-1");
        }
    }

    @Test
    public void ackOnCompletionAcksOnlyAfterSuccessfulFuture() throws Exception {
        RedisListenOperation op = new RedisListenOperation(streamContext("OnCompletion"));
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);
        ListenerExecutionResult success = mock(ListenerExecutionResult.class);
        when(success.isSuccess()).thenReturn(true);
        when(listener.submit(any(com.boomi.connector.api.Payload.class), any(SubmitOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(success));

        com.boomi.connector.redis.connection.RedisStreamMessage message =
                new com.boomi.connector.redis.connection.RedisStreamMessage("2-1", "v");

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, context) -> when(mock.xreadGroup(eq("s"), eq("mygroup"), anyString(), anyInt(), anyInt()))
                        .thenReturn(java.util.Collections.singletonList(message))
                        .thenReturn(java.util.Collections.emptyList()))) {
            op.start(listener, new RedisListenManager());
            Thread.sleep(200);
            op.stop();

            verify(mocked.constructed().get(0), atLeastOnce()).xack("s", "mygroup", "2-1");
        }
    }

    @Test
    public void ackOnCompletionDoesNotAckOnFailure() throws Exception {
        RedisListenOperation op = new RedisListenOperation(streamContext("OnCompletion"));
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);
        ListenerExecutionResult failure = mock(ListenerExecutionResult.class);
        when(failure.isSuccess()).thenReturn(false);
        when(listener.submit(any(com.boomi.connector.api.Payload.class), any(SubmitOptions.class)))
                .thenReturn(CompletableFuture.completedFuture(failure));

        com.boomi.connector.redis.connection.RedisStreamMessage message =
                new com.boomi.connector.redis.connection.RedisStreamMessage("3-1", "v");

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, context) -> when(mock.xreadGroup(eq("s"), eq("mygroup"), anyString(), anyInt(), anyInt()))
                        .thenReturn(java.util.Collections.singletonList(message))
                        .thenReturn(java.util.Collections.emptyList()))) {
            op.start(listener, new RedisListenManager());
            Thread.sleep(200);
            op.stop();

            verify(mocked.constructed().get(0), never()).xack(eq("s"), eq("mygroup"), eq("3-1"));
        }
    }

    @Test
    public void ackOnCompletionTimesOutAndDoesNotAckWhenFutureNeverCompletes() throws Exception {
        // Proves the future.get() bound documented on submitStreamMessage(): a process execution
        // that never completes must not hang the poll thread indefinitely -- it must give up after
        // the configured "Idle Reclaim Timeout" and leave the message unacknowledged for reclaim,
        // rather than blocking forever on an unbounded future.get().
        RedisListenOperation op = new RedisListenOperation(streamContext("OnCompletion", 50L));
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);

        @SuppressWarnings("unchecked")
        Future<ListenerExecutionResult> neverCompletingFuture = mock(Future.class);
        when(neverCompletingFuture.get(anyLong(), any(TimeUnit.class))).thenThrow(new TimeoutException());
        when(listener.submit(any(com.boomi.connector.api.Payload.class), any(SubmitOptions.class)))
                .thenReturn(neverCompletingFuture);

        com.boomi.connector.redis.connection.RedisStreamMessage message =
                new com.boomi.connector.redis.connection.RedisStreamMessage("4-1", "v");

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, context) -> when(mock.xreadGroup(eq("s"), eq("mygroup"), anyString(), anyInt(), anyInt()))
                        .thenReturn(java.util.Collections.singletonList(message))
                        .thenReturn(java.util.Collections.emptyList()))) {
            long startedAt = System.currentTimeMillis();
            op.start(listener, new RedisListenManager());
            Thread.sleep(200);
            op.stop();

            assertTrue("stop() must return promptly once the bounded future.get() gives up",
                    System.currentTimeMillis() - startedAt < 5000);
            verify(mocked.constructed().get(0), never()).xack(eq("s"), eq("mygroup"), eq("4-1"));
        }
    }

    @Test(timeout = 30000)
    public void stopJoinTimeoutCoversRealAckOnCompletionFutureWait() throws Exception {
        // Regression test for the Critical review finding: stop()'s join-timeout formula must
        // also cover submitStreamMessage()'s bounded future.get(ackWaitTimeoutMs, ...) wait, not
        // just the Redis-side blocking call budget -- otherwise stop() can race ahead, close()
        // redisConnection, and null it out while the poll thread is still parked in future.get(),
        // producing a raw NPE (or worse, a second concurrent poll thread) once that call returns.
        //
        // idle_reclaim_timeout_ms = 20_000ms => ackWaitTimeoutMs = 20_000. With poll_block_ms at
        // its 1_000ms default, the Redis-side component of the OLD (pre-fix) join formula is
        // max(10_000, 1_000 + 5_000) = 10_000ms -- it never considers ackWaitTimeoutMs at all. The
        // NEW formula additionally takes max(..., ackWaitTimeoutMs + 5_000) = max(10_000, 25_000)
        // = 25_000ms.
        //
        // The mocked Future's get(timeout, unit) does a REAL Thread.sleep(11_500ms) before
        // throwing TimeoutException -- not an instant answer -- so wall-clock time is what
        // actually discriminates old vs. new behavior here, per the lesson from the immediately
        // preceding task (an instantly-answering mock proves nothing about real timing races):
        //   - OLD formula (10_000ms join budget): join() times out at ~10s, while the poll thread
        //     is still 1.5s away from returning from its 11.5s real sleep inside future.get().
        //     stop() proceeds to redisConnection.close(); redisConnection = null while the poll
        //     thread is still in flight -- exactly the race the review finding describes.
        //   - NEW formula (25_000ms join budget): join() comfortably outlasts the 11.5s real wait,
        //     so the poll thread has already returned from future.get() (and, since the future
        //     throws TimeoutException, taken the timeout-log branch without touching
        //     redisConnection) by the time join() returns and stop() proceeds to close().
        //
        // Assertions: stop() must return within the new ~25s budget (not hang), no exception must
        // escape stop() (the outer runPollLoop() catch would swallow an NPE from a stale-connection
        // xack -- but there is no xack call on this path at all, since the future times out rather
        // than succeeding), and stop() must have genuinely waited out the real 11.5s in-flight
        // call rather than timing out early.
        RedisListenOperation op = new RedisListenOperation(streamContext("OnCompletion", 20_000L));
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);

        @SuppressWarnings("unchecked")
        Future<ListenerExecutionResult> slowFuture = mock(Future.class);
        when(slowFuture.get(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
            // A real sleep, matching the pattern proven in
            // stopJoinTimeoutScalesWithConfiguredPollBlockDuration -- must ignore interrupts too,
            // since stop()'s (harmless) interrupt() call must not shorten this wait and mask what
            // the join timeout math is actually being tested against.
            long deadline = System.currentTimeMillis() + 11_500L;
            while (System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(deadline - System.currentTimeMillis());
                } catch (InterruptedException ignored) {
                    // swallow, matching an uninterruptible blocking call
                }
            }
            throw new TimeoutException();
        });
        when(listener.submit(any(com.boomi.connector.api.Payload.class), any(SubmitOptions.class)))
                .thenReturn(slowFuture);

        com.boomi.connector.redis.connection.RedisStreamMessage message =
                new com.boomi.connector.redis.connection.RedisStreamMessage("5-1", "v");

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, context) -> when(mock.xreadGroup(eq("s"), eq("mygroup"), anyString(), anyInt(), anyInt()))
                        .thenReturn(java.util.Collections.singletonList(message))
                        .thenReturn(java.util.Collections.emptyList()))) {
            op.start(listener, new RedisListenManager());
            // Give the poll thread time to pick up the message and enter future.get() before
            // stop() is invoked, mirroring stop() racing a long-running in-flight submission.
            Thread.sleep(200L);

            long stopStartedAt = System.currentTimeMillis();
            op.stop();
            long stopElapsedMs = System.currentTimeMillis() - stopStartedAt;

            assertTrue("stop() must genuinely wait out the ~11.5s in-flight future.get() call via "
                    + "the extended join timeout rather than timing out early -- elapsed="
                    + stopElapsedMs + "ms (expected >= 11000ms)", stopElapsedMs >= 11_000L);
            verify(mocked.constructed().get(0), times(1)).close();
            verify(mocked.constructed().get(0), never()).xack(eq("s"), eq("mygroup"), eq("5-1"));
        }
    }

    @Test
    public void deadLetterKeyDefaultsToQueueKeyPlusSuffix() {
        RedisListenOperation op = new RedisListenOperation(streamContext("OnSubmission"));
        assertEquals("s:dead-letter", op.getDeadLetterKeyForTest());
    }

    @Test
    public void joinTimeoutIsCappedAt30SecondsEvenWithVeryLargeIdleReclaimTimeout() {
        // Regression test for the Important review finding: computeJoinTimeoutMs() used to scale
        // unboundedly with ackWaitTimeoutMs (sourced from idle_reclaim_timeout_ms in Stream +
        // ack-on-completion mode) -- an operator raising that field to accommodate slow processes
        // would silently make every listener stop() (Atom shutdown/redeploy) take proportionally
        // longer. This is safe to cap now because the generation counter, not the join timeout, is
        // what actually prevents a stale poll thread from resurrecting once stop() gives up.
        // idle_reclaim_timeout_ms = 300_000ms would (pre-fix) yield a join timeout of
        // max(10_000, 300_000 + 5_000) = 305_000ms; the fix must cap it at 30_000ms instead.
        RedisListenOperation op = new RedisListenOperation(streamContext("OnCompletion", 300_000L));
        assertEquals(30_000L, op.computeJoinTimeoutMsForTest());
    }

    @Test
    public void reclaimSweepRedeliversMessageUnderMaxAttempts() throws Exception {
        RedisListenOperation op = new RedisListenOperation(streamContext("OnSubmission"));
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);

        com.boomi.connector.redis.connection.RedisStreamPendingMessage pending =
                new com.boomi.connector.redis.connection.RedisStreamPendingMessage("4-1", 2L);
        com.boomi.connector.redis.connection.RedisStreamMessage claimed =
                new com.boomi.connector.redis.connection.RedisStreamMessage("4-1", "v");

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class, (mock, context) -> {
            when(mock.xreadGroup(anyString(), anyString(), anyString(), anyInt(), anyInt()))
                    .thenReturn(java.util.Collections.emptyList());
            when(mock.xpendingIdle(eq("s"), eq("mygroup"), anyLong(), anyInt()))
                    .thenReturn(java.util.Collections.singletonList(pending));
            when(mock.xclaim(eq("s"), eq("mygroup"), anyString(), eq("4-1"))).thenReturn(claimed);
        })) {
            op.start(listener, new RedisListenManager());
            Thread.sleep(200);
            op.stop();

            verify(mocked.constructed().get(0), atLeastOnce()).xack("s", "mygroup", "4-1");
            verify(mocked.constructed().get(0), never()).xadd(eq("s:dead-letter"), anyString());
            verify(listener, atLeastOnce()).submit(any(com.boomi.connector.api.Payload.class));
        }
    }

    @Test
    public void reclaimSweepDeadLettersMessageOverMaxAttempts() throws Exception {
        RedisListenOperation op = new RedisListenOperation(streamContext("OnSubmission"));
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);

        com.boomi.connector.redis.connection.RedisStreamPendingMessage pending =
                new com.boomi.connector.redis.connection.RedisStreamPendingMessage("5-1", 4L);
        com.boomi.connector.redis.connection.RedisStreamMessage claimed =
                new com.boomi.connector.redis.connection.RedisStreamMessage("5-1", "v");

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class, (mock, context) -> {
            when(mock.xreadGroup(anyString(), anyString(), anyString(), anyInt(), anyInt()))
                    .thenReturn(java.util.Collections.emptyList());
            when(mock.xpendingIdle(eq("s"), eq("mygroup"), anyLong(), anyInt()))
                    .thenReturn(java.util.Collections.singletonList(pending));
            when(mock.xclaim(eq("s"), eq("mygroup"), anyString(), eq("5-1"))).thenReturn(claimed);
        })) {
            op.start(listener, new RedisListenManager());
            Thread.sleep(200);
            op.stop();

            verify(mocked.constructed().get(0), atLeastOnce()).xadd(eq("s:dead-letter"), eq("v"));
            verify(mocked.constructed().get(0), atLeastOnce()).xack("s", "mygroup", "5-1");
            verify(listener, never()).submit(any(com.boomi.connector.api.Payload.class));
        }
    }

    @Test
    public void reclaimSweepDeadLettersMessageWhenClaimReachesMaxAttempts() throws Exception {
        // Regression test for the off-by-one in runReclaimSweep()'s dead-letter guard: XPENDING's
        // delivery count reflects deliveries BEFORE this sweep's XCLAIM call, but XCLAIM increments
        // the real delivery count as a side effect of claiming. With max_delivery_attempts=3 (see
        // streamContext), a message already delivered 3 times (pending.getDeliveryCount() == 3) is
        // about to become its 4th delivery once claimed here -- over the limit -- so it must be
        // dead-lettered on THIS sweep, not resubmitted for a 4th attempt.
        RedisListenOperation op = new RedisListenOperation(streamContext("OnSubmission"));
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);

        com.boomi.connector.redis.connection.RedisStreamPendingMessage pending =
                new com.boomi.connector.redis.connection.RedisStreamPendingMessage("6-1", 3L);
        com.boomi.connector.redis.connection.RedisStreamMessage claimed =
                new com.boomi.connector.redis.connection.RedisStreamMessage("6-1", "v");

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class, (mock, context) -> {
            when(mock.xreadGroup(anyString(), anyString(), anyString(), anyInt(), anyInt()))
                    .thenReturn(java.util.Collections.emptyList());
            when(mock.xpendingIdle(eq("s"), eq("mygroup"), anyLong(), anyInt()))
                    .thenReturn(java.util.Collections.singletonList(pending));
            when(mock.xclaim(eq("s"), eq("mygroup"), anyString(), eq("6-1"))).thenReturn(claimed);
        })) {
            op.start(listener, new RedisListenManager());
            Thread.sleep(200);
            op.stop();

            verify(mocked.constructed().get(0), atLeastOnce()).xadd(eq("s:dead-letter"), eq("v"));
            verify(mocked.constructed().get(0), atLeastOnce()).xack("s", "mygroup", "6-1");
            verify(listener, never()).submit(any(com.boomi.connector.api.Payload.class));
        }
    }

    @Test(timeout = 15000)
    public void reclaimSweepStopsProcessingRemainingEntriesOnceStoppingIsSignaled() throws Exception {
        // Regression test for the Critical review finding: runReclaimSweep()'s for-loop had no
        // stopping/interrupted check between iterations, so if stop() raced ahead while a sweep
        // was still working through multiple pending entries, redisConnection could be torn down
        // out from under the loop with no null guard at any of its four call sites. The fix adds
        // a stopping.get() check at the top of each iteration; this test proves that check
        // actually short-circuits the remaining entries once stop() is invoked mid-sweep, and
        // that no exception escapes. It intentionally does NOT test wall-clock join-timeout math
        // (that is already covered by the two prior timeout regression tests in this class) --
        // this fix's mechanism depends on the stopping flag being observed between iterations,
        // not on timeout arithmetic.
        RedisListenOperation op = new RedisListenOperation(streamContext("OnSubmission"));
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);

        com.boomi.connector.redis.connection.RedisStreamPendingMessage pending1 =
                new com.boomi.connector.redis.connection.RedisStreamPendingMessage("10-1", 1L);
        com.boomi.connector.redis.connection.RedisStreamPendingMessage pending2 =
                new com.boomi.connector.redis.connection.RedisStreamPendingMessage("10-2", 1L);
        com.boomi.connector.redis.connection.RedisStreamPendingMessage pending3 =
                new com.boomi.connector.redis.connection.RedisStreamPendingMessage("10-3", 1L);
        com.boomi.connector.redis.connection.RedisStreamMessage claimed1 =
                new com.boomi.connector.redis.connection.RedisStreamMessage("10-1", "v1");
        com.boomi.connector.redis.connection.RedisStreamMessage claimed2 =
                new com.boomi.connector.redis.connection.RedisStreamMessage("10-2", "v2");
        com.boomi.connector.redis.connection.RedisStreamMessage claimed3 =
                new com.boomi.connector.redis.connection.RedisStreamMessage("10-3", "v3");

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class, (mock, context) -> {
            when(mock.xreadGroup(anyString(), anyString(), anyString(), anyInt(), anyInt()))
                    .thenReturn(java.util.Collections.emptyList());
            when(mock.xpendingIdle(eq("s"), eq("mygroup"), anyLong(), anyInt()))
                    .thenReturn(java.util.Arrays.asList(pending1, pending2, pending3));
            // Processing the first pending entry takes real (short) time -- long enough for a
            // concurrently-invoked stop() to set `stopping` before this sweep's loop reaches
            // entry 2/3. Ignores interrupts, matching the "uninterruptible blocking call" pattern
            // used elsewhere in this class -- stop()'s (harmless) interrupt() call must not
            // shorten this wait.
            when(mock.xclaim(eq("s"), eq("mygroup"), anyString(), eq("10-1"))).thenAnswer(invocation -> {
                long deadline = System.currentTimeMillis() + 300L;
                while (System.currentTimeMillis() < deadline) {
                    try {
                        Thread.sleep(deadline - System.currentTimeMillis());
                    } catch (InterruptedException ignored) {
                        // swallow, matching an uninterruptible blocking call
                    }
                }
                return claimed1;
            });
            // Stubbed so that if the fix regresses and the loop keeps going past stop(), these
            // calls would succeed and the "never()" assertions below would fail loudly, rather
            // than silently passing because xclaim happened to return null for an unstubbed call.
            when(mock.xclaim(eq("s"), eq("mygroup"), anyString(), eq("10-2"))).thenReturn(claimed2);
            when(mock.xclaim(eq("s"), eq("mygroup"), anyString(), eq("10-3"))).thenReturn(claimed3);
        })) {
            op.start(listener, new RedisListenManager());
            // Let the poll thread begin the sweep and enter the ~300ms xclaim("10-1") call before
            // invoking stop() concurrently from this (the test) thread.
            Thread.sleep(100L);

            op.stop();

            RedisConnection connection = mocked.constructed().get(0);
            verify(connection, times(1)).xclaim(eq("s"), eq("mygroup"), anyString(), eq("10-1"));
            verify(connection, never()).xclaim(eq("s"), eq("mygroup"), anyString(), eq("10-2"));
            verify(connection, never()).xclaim(eq("s"), eq("mygroup"), anyString(), eq("10-3"));
            verify(listener, times(1)).submit(any(com.boomi.connector.api.Payload.class));
            verify(connection, times(1)).xack(eq("s"), eq("mygroup"), eq("10-1"));
            verify(connection, never()).xack(eq("s"), eq("mygroup"), eq("10-2"));
            verify(connection, never()).xack(eq("s"), eq("mygroup"), eq("10-3"));
            verify(connection, times(1)).close();
        }
    }

    @Test(timeout = 20000)
    public void staleThreadFromPriorGenerationDoesNotResumeLoopingAfterANewStart() throws Exception {
        // Regression test for the Important review finding (the most structurally significant fix
        // in this wave): if the FIRST poll thread is still blocked past stop()'s join timeout (a
        // slower-than-expected Redis call), stop() proceeds anyway and closes/nulls
        // redisConnection. A subsequent start() resets `stopping` back to false -- so, without a
        // generation guard, once the STALE thread's blocked call finally returns, it re-checks its
        // OLD loop condition, sees `stopping` is false again (set by the NEW start()), and resumes
        // looping -- now against the NEW redisConnection, concurrently with the NEW poll thread,
        // sharing a non-thread-safe Jedis instance between two threads.
        //
        // poll_block_ms = 2000 -> pollBlockSeconds = 2 -> computeJoinTimeoutMs()'s Redis-block
        // component is max(10_000, 2_000 + 5_000) = 10_000ms (the 10s floor). The first connection's
        // blpop() blocks for a REAL 10_800ms -- past the 10_000ms join budget -- so stop() (invoked
        // ~200ms after start()) times out at ~10_200ms elapsed while that thread is still blocked,
        // and proceeds to close() the first connection and null the field. A second start() then
        // runs (new generation, new connection); ~600ms later the stale first thread's blpop()
        // finally returns. With the generation guard, it must observe the generation mismatch and
        // exit WITHOUT ever touching the second connection. Without the guard, it would resume
        // looping and call blpop() on the second connection concurrently with the second poll
        // thread's own calls -- the second connection's blpop() stub below tracks concurrent
        // entries via a counter so that race is directly observable rather than inferred from call
        // counts alone.
        PropertyMap opProps = mock(PropertyMap.class);
        when(opProps.getProperty("queue_type", "List")).thenReturn("List");
        when(opProps.getProperty("queue_key", "")).thenReturn("q");
        when(opProps.getBooleanProperty("singleton_mode", Boolean.FALSE)).thenReturn(false);
        when(opProps.getLongProperty("poll_block_ms", 5000L)).thenReturn(2000L);
        OperationContext ctx = mock(OperationContext.class);
        when(ctx.getOperationProperties()).thenReturn(opProps);

        RedisListenOperation op = new RedisListenOperation(ctx);
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);
        RedisListenManager manager = new RedisListenManager();

        AtomicInteger activeSecondConnectionCalls = new AtomicInteger(0);
        AtomicInteger maxConcurrentSecondConnectionCalls = new AtomicInteger(0);

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, constructionContext) -> {
                    if (constructionContext.getCount() == 1) {
                        // First ("stale") connection: blocks past the join timeout, ignoring
                        // interrupts, matching this file's established "uninterruptible blocking
                        // socket read" pattern.
                        when(mock.blpop(eq("q"), anyInt())).thenAnswer(invocation -> {
                            long deadline = System.currentTimeMillis() + 10_800L;
                            while (System.currentTimeMillis() < deadline) {
                                try {
                                    Thread.sleep(deadline - System.currentTimeMillis());
                                } catch (InterruptedException ignored) {
                                    // swallow, matching an uninterruptible blocking call
                                }
                            }
                            return null;
                        });
                    } else {
                        // Second (current) connection: fast, repeating calls so the legitimate new
                        // poll thread is running at essentially 100% duty cycle by the time the
                        // stale thread's blocked call returns -- maximizing the chance a reverted
                        // fix's concurrent call is caught overlapping this one. Must ignore
                        // interrupts too, like the first connection's stub above: stop()'s
                        // (harmless) interrupt() call targets whichever thread is CURRENT at the
                        // time -- the second poll thread here -- and a real Jedis blpop() would
                        // not respond to it either (see stop()'s own comment on this). Propagating
                        // InterruptedException instead would make the poll thread take the
                        // exception-handling branch in runPollLoop() and its 5s backoff(), which
                        // would make this test's timing assertions flaky for reasons unrelated to
                        // the generation-counter fix under test.
                        when(mock.blpop(eq("q"), anyInt())).thenAnswer(invocation -> {
                            int current = activeSecondConnectionCalls.incrementAndGet();
                            try {
                                maxConcurrentSecondConnectionCalls.updateAndGet(
                                        max -> Math.max(max, current));
                                long deadline = System.currentTimeMillis() + 200L;
                                while (System.currentTimeMillis() < deadline) {
                                    try {
                                        Thread.sleep(deadline - System.currentTimeMillis());
                                    } catch (InterruptedException ignored) {
                                        // swallow, matching an uninterruptible blocking call
                                    }
                                }
                            } finally {
                                activeSecondConnectionCalls.decrementAndGet();
                            }
                            return null;
                        });
                    }
                })) {

            op.start(listener, manager);
            Thread.sleep(200L);

            op.stop();
            verify(mocked.constructed().get(0), times(1)).close();

            // Start a second time before the first (stale) poll thread's blpop() call has returned
            // -- it is still ~600ms away from its 10_800ms deadline at this point.
            op.start(listener, manager);

            // Wait past the point where the stale thread's blocked call returns (~10.8s from its
            // own start, i.e. well past this point in wall-clock time) plus enough margin for the
            // second poll thread to be in a steady polling state.
            Thread.sleep(1_800L);

            op.stop();

            // The stale first-generation thread's blocked blpop() call happened exactly once (the
            // initial call captured above); it must never call blpop() again afterward, on either
            // connection, once its generation is stale.
            verify(mocked.constructed().get(0), times(1)).blpop(eq("q"), anyInt());
            assertTrue("no more than one thread must ever be inside the second connection's "
                    + "blpop() at a time -- concurrency observed: " + maxConcurrentSecondConnectionCalls.get()
                    + " (a value of 2 means the stale first-generation thread resumed looping and "
                    + "called blpop() on the second connection concurrently with the current poll "
                    + "thread)",
                    maxConcurrentSecondConnectionCalls.get() <= 1);
            verify(mocked.constructed().get(1), times(1)).close();
        }
    }
}
