package com.boomi.connector.redis;

import com.boomi.connector.api.*;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class RedisBrowserListenSendTest {

    @Test
    public void listenOperationBrowsesToListenObjectType() {
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getOperationType()).thenReturn(OperationType.LISTEN);
        RedisConnection conn = mock(RedisConnection.class);
        RedisBrowser browser = new RedisBrowser(conn) {
            @Override public BrowseContext getContext() { return ctx; }
        };

        ObjectTypes types = browser.getObjectTypes();

        assertEquals(1, types.getTypes().size());
        assertEquals("Listen", types.getTypes().get(0).getId());
    }

    @Test
    public void executeOperationBrowsesToSendObjectType() {
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getOperationType()).thenReturn(OperationType.EXECUTE);
        RedisConnection conn = mock(RedisConnection.class);
        RedisBrowser browser = new RedisBrowser(conn) {
            @Override public BrowseContext getContext() { return ctx; }
        };

        ObjectTypes types = browser.getObjectTypes();

        assertEquals(1, types.getTypes().size());
        assertEquals("Send", types.getTypes().get(0).getId());
    }
}
