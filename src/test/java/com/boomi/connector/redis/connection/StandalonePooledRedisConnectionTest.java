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
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getProperty("authenticationType")).thenReturn("Basic");
        when(props.getProperty("user")).thenReturn("alice");
        when(props.getProperty("password")).thenReturn("pw");
        when(props.getBooleanProperty("poolEnabled")).thenReturn(true);
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
    public void closeReleasesPoolOnlyWhenLastReferenceGone() {
        StandalonePooledRedisConnection a = new StandalonePooledRedisConnection(basicConfig(), factory);
        StandalonePooledRedisConnection b = new StandalonePooledRedisConnection(basicConfig(), factory);
        a.close();
        verify(pool, never()).close();
        b.close();
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
