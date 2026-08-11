package com.boomi.connector.redis.connection;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.*;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class StandalonePooledRedisConnectionTest {

    private JedisClientFactory factory;
    private JedisPool pool;

    @Before
    public void setUp() {
        StandalonePooledRedisConnection.closeAllSharedPools();
        pool = mock(JedisPool.class);
        when(pool.isClosed()).thenReturn(false);
        factory = mock(JedisClientFactory.class);
        when(factory.createPool(any(GenericObjectPoolConfig.class), any(HostAndPort.class),
                any(JedisClientConfig.class))).thenReturn(pool);
    }

    @After
    public void tearDown() {
        StandalonePooledRedisConnection.closeAllSharedPools();
    }

    private RedisConnectionConfig basicConfig() {
        return basicConfig("pw");
    }

    private RedisConnectionConfig basicConfig(String password) {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getProperty("authenticationType")).thenReturn("Basic");
        when(props.getProperty("user")).thenReturn("alice");
        when(props.getProperty("password")).thenReturn(password);
        when(props.getBooleanProperty("poolEnabled", Boolean.FALSE)).thenReturn(true);
        when(props.getLongProperty("poolSize", 4L)).thenReturn(4L);
        when(props.getLongProperty("minPoolSize", 1L)).thenReturn(1L);
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        return new RedisConnectionConfig(ctx);
    }

    @Test
    public void createsPoolOnceAndReusesByKey() {
        StandalonePooledRedisConnection a = new StandalonePooledRedisConnection(basicConfig(), factory);
        StandalonePooledRedisConnection b = new StandalonePooledRedisConnection(basicConfig(), factory);
        verify(factory, times(1)).createPool(any(), any(), any());
        assertEquals(1, StandalonePooledRedisConnection.getSharedPoolCount());
        a.close();
        b.close();
    }

    @Test
    public void closeNeverClosesTheSharedPoolEvenAsTheLastReference() {
        // The pool must survive across executions when pooling is enabled - it is a long-lived
        // resource, not scoped to any single instance. Only closeAllSharedPools() may close it.
        StandalonePooledRedisConnection a = new StandalonePooledRedisConnection(basicConfig(), factory);
        StandalonePooledRedisConnection b = new StandalonePooledRedisConnection(basicConfig(), factory);
        a.close();
        b.close();
        verify(pool, never()).close();
        assertEquals(1, StandalonePooledRedisConnection.getSharedPoolCount());
    }

    @Test
    public void sequentialExecutionsReuseTheSamePoolInstance() {
        // Regression test: an execution closing its connection must not tear down the shared pool
        // out from under the next sequential execution against the same connection configuration.
        StandalonePooledRedisConnection a = new StandalonePooledRedisConnection(basicConfig(), factory);
        a.close();

        StandalonePooledRedisConnection b = new StandalonePooledRedisConnection(basicConfig(), factory);
        b.close();

        verify(factory, times(1)).createPool(any(), any(), any());
        verify(pool, never()).close();
    }

    @Test
    public void credentialChangeEvictsTheSupersededIdlePoolForTheSameEndpoint() {
        StandalonePooledRedisConnection a = new StandalonePooledRedisConnection(basicConfig("pw"), factory);
        a.close(); // no in-flight execution holds the old pool

        JedisPool rotatedPool = mock(JedisPool.class);
        when(rotatedPool.isClosed()).thenReturn(false);
        when(factory.createPool(any(GenericObjectPoolConfig.class), any(HostAndPort.class),
                any(JedisClientConfig.class))).thenReturn(rotatedPool);
        StandalonePooledRedisConnection b = new StandalonePooledRedisConnection(basicConfig("rotated-pw"), factory);

        // The stale-credential pool must not linger (its evictor would re-AUTH with dead
        // credentials forever); only the rotated pool remains registered.
        verify(pool, times(1)).close();
        verify(rotatedPool, never()).close();
        assertEquals(1, StandalonePooledRedisConnection.getSharedPoolCount());
        b.close();
    }

    @Test
    public void credentialChangeDoesNotEvictAPoolStillHeldByAnInFlightExecution() {
        StandalonePooledRedisConnection a = new StandalonePooledRedisConnection(basicConfig("pw"), factory);
        // a stays open: an in-flight execution still holds the old pool

        JedisPool rotatedPool = mock(JedisPool.class);
        when(rotatedPool.isClosed()).thenReturn(false);
        when(factory.createPool(any(GenericObjectPoolConfig.class), any(HostAndPort.class),
                any(JedisClientConfig.class))).thenReturn(rotatedPool);
        StandalonePooledRedisConnection b = new StandalonePooledRedisConnection(basicConfig("rotated-pw"), factory);

        verify(pool, never()).close();
        assertEquals(2, StandalonePooledRedisConnection.getSharedPoolCount());
        a.close();
        b.close();
    }

    @Test
    public void closeAllSharedPoolsClosesAPoolEvenAfterEveryInstanceAlreadyClosed() {
        StandalonePooledRedisConnection a = new StandalonePooledRedisConnection(basicConfig(), factory);
        a.close();

        StandalonePooledRedisConnection.closeAllSharedPools();

        verify(pool, times(1)).close();
        assertEquals(0, StandalonePooledRedisConnection.getSharedPoolCount());
    }

    @Test
    public void getBorrowsFromPool() {
        Jedis jedis = mock(Jedis.class);
        when(pool.getResource()).thenReturn(jedis);
        when(jedis.get("k")).thenReturn("v");
        StandalonePooledRedisConnection a = new StandalonePooledRedisConnection(basicConfig(), factory);
        assertEquals("v", a.get("k"));
        a.close();
    }

    @Test
    public void closeDoesNotAffectAnUnrelatedPoolRegisteredUnderTheSameKeyAfterReplacement() {
        StandalonePooledRedisConnection a = new StandalonePooledRedisConnection(basicConfig(), factory);

        // Simulate closeAllSharedPools() running (e.g. shutdown/test teardown elsewhere) while `a`
        // still holds a reference to the now-discarded original pool, followed by a new instance
        // registering a fresh pool under the identical key.
        StandalonePooledRedisConnection.closeAllSharedPools();
        JedisPool newPool = mock(JedisPool.class);
        when(newPool.isClosed()).thenReturn(false);
        when(factory.createPool(any(GenericObjectPoolConfig.class), any(HostAndPort.class),
                any(JedisClientConfig.class))).thenReturn(newPool);
        StandalonePooledRedisConnection b = new StandalonePooledRedisConnection(basicConfig(), factory);

        a.close();

        verify(newPool, never()).close();
        assertEquals(1, StandalonePooledRedisConnection.getSharedPoolCount());
        b.close();
    }

    @Test
    public void delAllUsesPipelinedSingleKeyDeletes() {
        Jedis jedis = mock(Jedis.class);
        Pipeline pipeline = mock(Pipeline.class);
        when(pool.getResource()).thenReturn(jedis);
        when(jedis.pipelined()).thenReturn(pipeline);
        when(jedis.scan(eq(ScanParams.SCAN_POINTER_START), any(ScanParams.class)))
                .thenReturn(new ScanResult<>(ScanParams.SCAN_POINTER_START, Arrays.asList("a", "b")));

        StandalonePooledRedisConnection a = new StandalonePooledRedisConnection(basicConfig(), factory);
        a.delAll("prefix:");

        verify(pipeline).del("a");
        verify(pipeline).del("b");
        verify(pipeline).sync();
        verify(jedis, never()).del(any(String[].class));
        a.close();
    }
}
