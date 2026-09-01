package com.boomi.connector.redis.connection;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.redis.pool.RedisClientPoolManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class StandalonePooledRedisConnectionQueueTest {

    @Before
    public void setUp() {
        RedisClientPoolManager.closeAll();
    }

    @After
    public void tearDown() {
        RedisClientPoolManager.closeAll();
    }

    private RedisConnectionConfig pooledConfig() {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getProperty("authenticationType")).thenReturn("None");
        when(props.getProperty("clusteringPolicy")).thenReturn("NonClustered");
        when(props.getBooleanProperty("poolEnabled", Boolean.FALSE)).thenReturn(true);
        when(props.getLongProperty("poolSize", 4L)).thenReturn(4L);
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        return new RedisConnectionConfig(ctx);
    }

    private StandalonePooledRedisConnection connectionWithPooledJedis(Jedis jedis) {
        JedisPool pool = mock(JedisPool.class);
        when(pool.getResource()).thenReturn(jedis);
        JedisClientFactory factory = mock(JedisClientFactory.class);
        when(factory.createPool(any(), any(HostAndPort.class), any(JedisClientConfig.class))).thenReturn(pool);
        return new StandalonePooledRedisConnection(pooledConfig(), factory);
    }

    @Test
    public void rpushBorrowsFromPoolAndDelegates() {
        Jedis jedis = mock(Jedis.class);
        StandalonePooledRedisConnection conn = connectionWithPooledJedis(jedis);
        conn.rpush("q", "v");
        verify(jedis).rpush("q", "v");
        verify(jedis).close();
    }

    @Test
    public void blpopBorrowsFromPoolAndReturnsValue() {
        Jedis jedis = mock(Jedis.class);
        when(jedis.blpop(5, "q")).thenReturn(java.util.Arrays.asList("q", "v"));
        StandalonePooledRedisConnection conn = connectionWithPooledJedis(jedis);
        assertEquals("v", conn.blpop("q", 5));
    }

    @Test
    public void xgroupCreateSwallowsBusygroup() {
        Jedis jedis = mock(Jedis.class);
        when(jedis.xgroupCreate(eq("s"), eq("g"), any(StreamEntryID.class), eq(true)))
                .thenThrow(new redis.clients.jedis.exceptions.JedisDataException("BUSYGROUP already exists"));
        StandalonePooledRedisConnection conn = connectionWithPooledJedis(jedis);
        conn.xgroupCreate("s", "g");
    }

    @Test
    public void xaddSendsValueField() {
        Jedis jedis = mock(Jedis.class);
        StandalonePooledRedisConnection conn = connectionWithPooledJedis(jedis);
        conn.xadd("s", "hello");
        verify(jedis).xadd(eq("s"), eq(StreamEntryID.NEW_ENTRY),
                eq(java.util.Collections.singletonMap("value", "hello")));
    }

    @Test
    public void xackAcknowledgesById() {
        Jedis jedis = mock(Jedis.class);
        StandalonePooledRedisConnection conn = connectionWithPooledJedis(jedis);
        conn.xack("s", "g", "1-1");
        verify(jedis).xack("s", "g", new StreamEntryID("1-1"));
    }

    @Test
    public void xclaimReturnsNullWhenNoLongerPending() {
        Jedis jedis = mock(Jedis.class);
        when(jedis.xclaim(eq("s"), eq("g"), eq("c"), eq(0L), any(), eq(new StreamEntryID("3-1"))))
                .thenReturn(java.util.Collections.emptyList());
        StandalonePooledRedisConnection conn = connectionWithPooledJedis(jedis);
        assertNull(conn.xclaim("s", "g", "c", "3-1"));
    }
}
