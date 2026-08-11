package com.boomi.connector.redis.pool;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.redis.connection.RedisConnectionConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RedisClientPoolManagerTest {

    @Before
    public void resetBefore() {
        RedisClientPoolManager.closeAll();
    }

    @After
    public void resetAfter() {
        RedisClientPoolManager.closeAll();
    }

    private static RedisClientSettings settings(String componentId) {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("id")).thenReturn(componentId);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getProperty("authenticationType")).thenReturn("None");
        when(props.getBooleanProperty("poolEnabled", Boolean.FALSE)).thenReturn(true);
        when(props.getLongProperty("poolSize", 4L)).thenReturn(4L);
        when(props.getLongProperty("minPoolSize", 1L)).thenReturn(1L);
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        return new RedisClientSettings(new RedisConnectionConfig(ctx));
    }

    @Test
    public void acquireBuildsOnceAndReusesForEqualSettings() {
        Closeable client = mock(Closeable.class);
        AtomicInteger builds = new AtomicInteger();

        Closeable first = RedisClientPoolManager.acquire(settings("comp-1"), () -> {
            builds.incrementAndGet();
            return client;
        });
        Closeable second = RedisClientPoolManager.acquire(settings("comp-1"), () -> {
            builds.incrementAndGet();
            return client;
        });

        assertSame(client, first);
        assertSame(client, second);
        assertEquals(1, builds.get());
        assertEquals(1, RedisClientPoolManager.getActiveClientCount());
        assertEquals(2, RedisClientPoolManager.getActiveReferences(settings("comp-1")));
    }

    @Test
    public void differentComponentIdsGetIndependentClients() {
        Closeable clientA = mock(Closeable.class);
        Closeable clientB = mock(Closeable.class);

        Closeable a = RedisClientPoolManager.acquire(settings("comp-1"), () -> clientA);
        Closeable b = RedisClientPoolManager.acquire(settings("comp-2"), () -> clientB);

        assertSame(clientA, a);
        assertSame(clientB, b);
        assertEquals(2, RedisClientPoolManager.getActiveClientCount());
    }

    @Test
    public void releaseDecrementsButNeverClosesTheClient() throws Exception {
        Closeable client = mock(Closeable.class);
        RedisClientPoolManager.acquire(settings("comp-1"), () -> client);

        RedisClientPoolManager.release(settings("comp-1"), client);

        verify(client, never()).close();
        assertEquals(1, RedisClientPoolManager.getActiveClientCount());
        assertEquals(0, RedisClientPoolManager.getActiveReferences(settings("comp-1")));
    }

    @Test
    public void sequentialAcquireReleaseCyclesReuseTheSameClient() {
        Closeable client = mock(Closeable.class);
        AtomicInteger builds = new AtomicInteger();

        for (int i = 0; i < 3; i++) {
            Closeable acquired = RedisClientPoolManager.acquire(settings("comp-1"), () -> {
                builds.incrementAndGet();
                return client;
            });
            RedisClientPoolManager.release(settings("comp-1"), acquired);
        }

        assertEquals("the client must survive refcount hitting zero between executions",
                1, builds.get());
    }

    @Test
    public void releaseWithStaleClientInstanceDoesNotTouchCurrentEntry() {
        // A holds client1; closeAll() runs (shutdown/test elsewhere); B registers client2 under
        // the same settings; A's late release must not decrement client2's references.
        Closeable client1 = mock(Closeable.class);
        Closeable acquired1 = RedisClientPoolManager.acquire(settings("comp-1"), () -> client1);
        RedisClientPoolManager.closeAll();

        Closeable client2 = mock(Closeable.class);
        RedisClientPoolManager.acquire(settings("comp-1"), () -> client2);

        RedisClientPoolManager.release(settings("comp-1"), acquired1);

        assertEquals(1, RedisClientPoolManager.getActiveReferences(settings("comp-1")));
    }

    @Test
    public void acquireAfterCloseAllRebuilds() throws Exception {
        Closeable client1 = mock(Closeable.class);
        RedisClientPoolManager.acquire(settings("comp-1"), () -> client1);
        RedisClientPoolManager.closeAll();
        verify(client1).close();

        Closeable client2 = mock(Closeable.class);
        Closeable acquired = RedisClientPoolManager.acquire(settings("comp-1"), () -> client2);
        assertSame(client2, acquired);
    }

    @Test
    public void closeAllClosesEverythingAndEmptiesTheRegistry() throws Exception {
        Closeable clientA = mock(Closeable.class);
        Closeable clientB = mock(Closeable.class);
        RedisClientPoolManager.acquire(settings("comp-1"), () -> clientA);
        RedisClientPoolManager.acquire(settings("comp-2"), () -> clientB);

        RedisClientPoolManager.closeAll();

        verify(clientA).close();
        verify(clientB).close();
        assertEquals(0, RedisClientPoolManager.getActiveClientCount());
    }

    @Test
    public void getActiveReferencesReturnsMinusOneForUnknownSettings() {
        assertEquals(-1, RedisClientPoolManager.getActiveReferences(settings("never-acquired")));
    }
}
