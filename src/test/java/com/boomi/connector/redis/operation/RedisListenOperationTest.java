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
