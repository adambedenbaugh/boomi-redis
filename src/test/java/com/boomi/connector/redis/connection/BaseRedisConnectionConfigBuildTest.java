package com.boomi.connector.redis.connection;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.redis.authentication.BoomiRedisCredentialsProvider;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Test;
import redis.clients.jedis.Connection;
import redis.clients.jedis.JedisClientConfig;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class BaseRedisConnectionConfigBuildTest {

    /** Minimal concrete subclass exposing buildClientConfig() for assertions. */
    private static class ProbeConnection extends BaseRedisConnection {
        ProbeConnection(RedisConnectionConfig config) { super(config, new DefaultJedisClientFactory()); }
        JedisClientConfig probe() { return buildClientConfig(); }
        GenericObjectPoolConfig<Connection> probePool() { return createConnectionPoolConfig(); }
        public String get(String k) { return null; }
        public void set(String k, String v, Long ttl) { }
        public void del(String k) { }
        public void delAll(String p) { }
        public java.util.Map<String,String> getAll(String p) { return null; }
        public void close() { }
    }

    private static RedisConnectionConfig config(String authType) {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getProperty("authenticationType")).thenReturn(authType);
        when(props.getBooleanProperty("useSSL", Boolean.FALSE)).thenReturn(true);
        when(props.getLongProperty("connectionTimeout", 5L)).thenReturn(5L);
        when(props.getLongProperty("socketTimeout", 5L)).thenReturn(7L);
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        return new RedisConnectionConfig(ctx);
    }

    @Test
    public void noneAuthAttachesNoCredentialsProvider() {
        // NOTE: Jedis 5.2.0's DefaultJedisClientConfig.Builder#build() always defaults
        // credentialsProvider to a non-null DefaultRedisCredentialsProvider wrapping
        // (null, null) when none was explicitly set (verified via bytecode inspection of
        // jedis-5.2.0.jar), so getCredentialsProvider() itself is never null. The
        // observable contract we actually care about is that our BoomiRedisCredentialsProvider
        // is not wired in for NONE (it would otherwise attempt Entra/Basic credential
        // resolution). Jedis's own default wraps null credentials, which its internal
        // auth() short-circuits on a null password, so no AUTH command is sent either way.
        JedisClientConfig cfg = new ProbeConnection(config("None")).probe();
        assertFalse("NONE must not attach the Boomi credentials provider",
                cfg.getCredentialsProvider() instanceof BoomiRedisCredentialsProvider);
        assertTrue(cfg.isSsl());
        assertEquals(5000, cfg.getConnectionTimeoutMillis());
        assertEquals(7000, cfg.getSocketTimeoutMillis());
    }

    @Test
    public void basicAuthAttachesCredentialsProvider() {
        // assertNotNull alone would not detect a missing attachment, since Jedis's builder
        // always defaults getCredentialsProvider() to a non-null wrapper (see note above).
        // Assert that our own provider is the one that got attached.
        JedisClientConfig cfg = new ProbeConnection(config("Basic")).probe();
        assertTrue("Basic must attach the Boomi credentials provider",
                cfg.getCredentialsProvider() instanceof BoomiRedisCredentialsProvider);
    }

    @Test
    public void clusterPoolMaxTotalIsPositiveWhenPoolingDisabled() {
        GenericObjectPoolConfig<Connection> pool = new ProbeConnection(config("None")).probePool();
        assertEquals(8, pool.getMaxTotal());
        assertTrue("minIdle must not exceed maxTotal", pool.getMinIdle() <= pool.getMaxTotal());
    }

    @Test
    public void clusterPoolMinIdleClampedToMaxTotal() {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getProperty("authenticationType")).thenReturn("None");
        when(props.getLongProperty("minPoolSize", 1L)).thenReturn(20L);
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);

        GenericObjectPoolConfig<Connection> pool = new ProbeConnection(new RedisConnectionConfig(ctx)).probePool();
        assertEquals(8, pool.getMaxTotal());
        assertEquals("minIdle must be clamped to maxTotal", 8, pool.getMinIdle());
    }
}
