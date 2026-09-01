package com.boomi.connector.redis.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.listen.Listener;
import com.boomi.connector.api.listen.ListenerStatus;
import com.boomi.connector.redis.RedisConnection;
import com.boomi.connector.redis.pool.RedisListenManager;
import org.junit.Test;
import org.mockito.MockedConstruction;

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
}
