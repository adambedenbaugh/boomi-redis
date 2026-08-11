package com.boomi.connector.redis.pool;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.redis.connection.RedisConnectionConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
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

    /** Builds settings keyed by the given hosts value - distinct hosts means a distinct pool key. */
    private static RedisClientSettings settings(String hosts) {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn(hosts);
        when(props.getProperty("authenticationType")).thenReturn("None");
        when(props.getBooleanProperty("poolEnabled", Boolean.FALSE)).thenReturn(true);
        when(props.getLongProperty("poolSize", 4L)).thenReturn(4L);
        when(props.getLongProperty("minPoolSize", 1L)).thenReturn(1L);
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        return new RedisClientSettings(new RedisConnectionConfig(ctx));
    }

    private static RedisClientSettings settingsWithPassword(String hosts, String password) {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn(hosts);
        when(props.getProperty("authenticationType")).thenReturn("Basic");
        when(props.getProperty("user")).thenReturn("alice");
        when(props.getProperty("password")).thenReturn(password);
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
    public void differentSettingsGetIndependentClients() {
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

    /**
     * Pins the invariant the evictor's thread-safety now relies on (see the class Javadoc on
     * {@code RedisClientPoolManager}): the lock object registered for a settings key is created
     * exactly once and is never removed, so an eviction can never leave behind a "gap" where a
     * concurrent acquire() would synchronize on a different monitor than the evictor holds. Not a
     * true concurrent stress test, but it does exercise the real eviction path end to end (via
     * the package-visible {@code runEviction} hook) and asserts on the lock object's identity
     * directly, rather than only on its externally observable effects.
     */
    @Test
    public void lockObjectIdentityIsStableAcrossEvictionAndRebuild() throws Exception {
        RedisClientSettings settings = settings("comp-1");
        Closeable client1 = mock(Closeable.class);

        Closeable acquired1 = RedisClientPoolManager.acquire(settings, () -> client1);
        Object lockBeforeEviction = RedisClientPoolManager.getLockObject(settings);
        assertNotNull("acquire() must register a lock object for the settings key", lockBeforeEviction);

        RedisClientPoolManager.release(settings, acquired1);
        assertEquals(0, RedisClientPoolManager.getActiveReferences(settings));

        // Drive the evictor far enough past CLIENT_EXPIRATION_INTERVAL_MILLIS to close client1.
        long farFuture = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(31);
        RedisClientPoolManager.runEviction(farFuture);

        verify(client1).close();
        assertEquals("the evicted entry must be gone from ACTIVE_CLIENTS",
                -1, RedisClientPoolManager.getActiveReferences(settings));

        Object lockAfterEviction = RedisClientPoolManager.getLockObject(settings);
        assertSame("the lock object must survive eviction unchanged - a new/different lock object "
                        + "here would mean a concurrent acquire() could synchronize on a monitor "
                        + "the evictor was never holding",
                lockBeforeEviction, lockAfterEviction);

        Closeable client2 = mock(Closeable.class);
        Closeable acquired2 = RedisClientPoolManager.acquire(settings, () -> client2);

        assertSame("acquire() after eviction must rebuild with a fresh client", client2, acquired2);
        assertSame("acquire() after eviction must reuse the very same lock object",
                lockBeforeEviction, RedisClientPoolManager.getLockObject(settings));
    }

    @Test
    public void evictionClosesAndRemovesExpiredIdleClients() throws Exception {
        Closeable client = mock(Closeable.class);
        RedisClientPoolManager.acquire(settings("comp-1"), () -> client);
        RedisClientPoolManager.release(settings("comp-1"), client);

        long lastUsed = System.currentTimeMillis();
        long afterExpiry = lastUsed + RedisClientPoolManager.CLIENT_EXPIRATION_INTERVAL_MILLIS + 1_000;
        RedisClientPoolManager.runEviction(afterExpiry);

        verify(client).close();
        assertEquals(0, RedisClientPoolManager.getActiveClientCount());
    }

    @Test
    public void evictionSkipsClientsWithActiveReferencesRegardlessOfAge() throws Exception {
        Closeable client = mock(Closeable.class);
        RedisClientPoolManager.acquire(settings("comp-1"), () -> client);
        // NOT released - an execution still holds it.

        long farFuture = System.currentTimeMillis()
                + (10 * RedisClientPoolManager.CLIENT_EXPIRATION_INTERVAL_MILLIS);
        RedisClientPoolManager.runEviction(farFuture);

        verify(client, never()).close();
        assertEquals(1, RedisClientPoolManager.getActiveClientCount());
    }

    @Test
    public void evictionSkipsRecentlyUsedIdleClients() throws Exception {
        Closeable client = mock(Closeable.class);
        RedisClientPoolManager.acquire(settings("comp-1"), () -> client);
        RedisClientPoolManager.release(settings("comp-1"), client);

        long justUnderExpiry = System.currentTimeMillis()
                + RedisClientPoolManager.CLIENT_EXPIRATION_INTERVAL_MILLIS - 60_000;
        RedisClientPoolManager.runEviction(justUnderExpiry);

        verify(client, never()).close();
        assertEquals(1, RedisClientPoolManager.getActiveClientCount());
    }

    @Test
    public void supersededClientDiesOffAfterConfigChangeWhileNewClientLives() throws Exception {
        // The credential-rotation scenario end to end: old settings' client goes idle, new
        // settings' client is in active use; eviction reaps only the old one.
        Closeable oldClient = mock(Closeable.class);
        RedisClientPoolManager.acquire(settings("comp-1"), () -> oldClient);
        RedisClientPoolManager.release(settings("comp-1"), oldClient);

        RedisClientSettings rotated = settingsWithPassword("comp-1", "new-password");
        Closeable newClient = mock(Closeable.class);
        RedisClientPoolManager.acquire(rotated, () -> newClient);

        long afterExpiry = System.currentTimeMillis()
                + RedisClientPoolManager.CLIENT_EXPIRATION_INTERVAL_MILLIS + 1_000;
        RedisClientPoolManager.runEviction(afterExpiry);

        verify(oldClient).close();
        verify(newClient, never()).close();
        assertEquals(1, RedisClientPoolManager.getActiveClientCount());
    }

    @Test
    public void acquireAfterEvictionRebuildsTheClient() {
        Closeable client1 = mock(Closeable.class);
        RedisClientPoolManager.acquire(settings("comp-1"), () -> client1);
        RedisClientPoolManager.release(settings("comp-1"), client1);
        RedisClientPoolManager.runEviction(System.currentTimeMillis()
                + RedisClientPoolManager.CLIENT_EXPIRATION_INTERVAL_MILLIS + 1_000);

        Closeable client2 = mock(Closeable.class);
        Closeable acquired = RedisClientPoolManager.acquire(settings("comp-1"), () -> client2);

        assertSame(client2, acquired);
        assertEquals(1, RedisClientPoolManager.getActiveReferences(settings("comp-1")));
    }
}
