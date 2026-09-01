package com.boomi.connector.redis;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.redis.connection.RedisConnectionInterface;
import com.boomi.connector.redis.connection.RedisStreamMessage;
import com.boomi.connector.redis.connection.RedisStreamPendingMessage;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RedisConnectionQueueTest {

    private RedisConnection connection;
    private RedisConnectionInterface mockClient;

    @Before
    public void setUp() throws Exception {
        BrowseContext ctx = mock(BrowseContext.class);
        connection = new RedisConnection(ctx);
        mockClient = mock(RedisConnectionInterface.class);
        // Bypass init()/RedisConnectionFactory by injecting the mock client directly.
        Field field = RedisConnection.class.getDeclaredField("redisClient");
        field.setAccessible(true);
        field.set(connection, mockClient);
    }

    @Test
    public void rpushDelegates() {
        connection.rpush("q", "v");
        verify(mockClient).rpush("q", "v");
    }

    @Test
    public void blpopDelegatesAndReturnsValue() {
        when(mockClient.blpop("q", 5)).thenReturn("v");
        assertEquals("v", connection.blpop("q", 5));
    }

    @Test
    public void xgroupCreateDelegates() {
        connection.xgroupCreate("s", "g");
        verify(mockClient).xgroupCreate("s", "g");
    }

    @Test
    public void xaddDelegates() {
        connection.xadd("s", "v");
        verify(mockClient).xadd("s", "v");
    }

    @Test
    public void xreadGroupDelegatesAndReturnsMessages() {
        List<RedisStreamMessage> expected = Collections.singletonList(new RedisStreamMessage("1-1", "v"));
        when(mockClient.xreadGroup("s", "g", "c", 1, 1000)).thenReturn(expected);
        assertEquals(expected, connection.xreadGroup("s", "g", "c", 1, 1000));
    }

    @Test
    public void xackDelegates() {
        connection.xack("s", "g", "1-1");
        verify(mockClient).xack("s", "g", "1-1");
    }

    @Test
    public void xpendingIdleDelegates() {
        List<RedisStreamPendingMessage> expected = Collections.singletonList(new RedisStreamPendingMessage("1-1", 2L));
        when(mockClient.xpendingIdle("s", "g", 60000L, 100)).thenReturn(expected);
        assertEquals(expected, connection.xpendingIdle("s", "g", 60000L, 100));
    }

    @Test
    public void xclaimDelegates() {
        RedisStreamMessage expected = new RedisStreamMessage("1-1", "v");
        when(mockClient.xclaim("s", "g", "c", "1-1")).thenReturn(expected);
        assertEquals(expected, connection.xclaim("s", "g", "c", "1-1"));
    }
}
