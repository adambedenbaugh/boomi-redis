package com.boomi.connector.redis.connection;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import org.junit.Test;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.XClaimParams;
import redis.clients.jedis.params.XPendingParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamPendingEntry;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class StandaloneRedisConnectionQueueTest {

    private RedisConnectionConfig standaloneConfig() {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getProperty("authenticationType")).thenReturn("None");
        when(props.getProperty("clusteringPolicy")).thenReturn("NonClustered");
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        return new RedisConnectionConfig(ctx);
    }

    private StandaloneRedisConnection connectionWithJedis(Jedis jedis) {
        JedisClientFactory factory = mock(JedisClientFactory.class);
        when(factory.createClient(any(HostAndPort.class), any(JedisClientConfig.class))).thenReturn(jedis);
        return new StandaloneRedisConnection(standaloneConfig(), factory);
    }

    @Test
    public void rpushDelegatesToJedis() {
        Jedis jedis = mock(Jedis.class);
        StandaloneRedisConnection conn = connectionWithJedis(jedis);
        conn.rpush("q", "v");
        verify(jedis).rpush("q", "v");
    }

    @Test
    public void blpopReturnsValueFromTwoElementResult() {
        Jedis jedis = mock(Jedis.class);
        when(jedis.blpop(5, "q")).thenReturn(Arrays.asList("q", "v"));
        StandaloneRedisConnection conn = connectionWithJedis(jedis);
        assertEquals("v", conn.blpop("q", 5));
    }

    @Test
    public void blpopReturnsNullOnTimeout() {
        Jedis jedis = mock(Jedis.class);
        when(jedis.blpop(5, "q")).thenReturn(null);
        StandaloneRedisConnection conn = connectionWithJedis(jedis);
        assertNull(conn.blpop("q", 5));
    }

    @Test
    public void xgroupCreateSwallowsBusygroup() {
        Jedis jedis = mock(Jedis.class);
        when(jedis.xgroupCreate(eq("s"), eq("g"), any(StreamEntryID.class), eq(true)))
                .thenThrow(new JedisDataException("BUSYGROUP Consumer Group name already exists"));
        StandaloneRedisConnection conn = connectionWithJedis(jedis);
        conn.xgroupCreate("s", "g");
    }

    @Test(expected = JedisDataException.class)
    public void xgroupCreateRethrowsOtherErrors() {
        Jedis jedis = mock(Jedis.class);
        when(jedis.xgroupCreate(eq("s"), eq("g"), any(StreamEntryID.class), eq(true)))
                .thenThrow(new JedisDataException("ERR something else"));
        StandaloneRedisConnection conn = connectionWithJedis(jedis);
        conn.xgroupCreate("s", "g");
    }

    @Test
    public void xaddSendsValueField() {
        Jedis jedis = mock(Jedis.class);
        StandaloneRedisConnection conn = connectionWithJedis(jedis);
        conn.xadd("s", "hello");
        verify(jedis).xadd(eq("s"), eq(StreamEntryID.NEW_ENTRY), eq(Collections.singletonMap("value", "hello")));
    }

    @Test
    public void xreadGroupFlattensEntriesToMessages() {
        Jedis jedis = mock(Jedis.class);
        StreamEntry entry = new StreamEntry(new StreamEntryID("1-1"), Collections.singletonMap("value", "v1"));
        List<Map.Entry<String, List<StreamEntry>>> raw = Collections.singletonList(
                (Map.Entry<String, List<StreamEntry>>) new AbstractMap.SimpleEntry<>("s", Collections.singletonList(entry)));
        when(jedis.xreadGroup(eq("g"), eq("c"), any(XReadGroupParams.class), any(Map.class))).thenReturn(raw);
        StandaloneRedisConnection conn = connectionWithJedis(jedis);

        List<RedisStreamMessage> messages = conn.xreadGroup("s", "g", "c", 1, 1000);

        assertEquals(1, messages.size());
        assertEquals("1-1", messages.get(0).getId());
        assertEquals("v1", messages.get(0).getValue());
    }

    @Test
    public void xackAcknowledgesById() {
        Jedis jedis = mock(Jedis.class);
        StandaloneRedisConnection conn = connectionWithJedis(jedis);
        conn.xack("s", "g", "1-1");
        verify(jedis).xack("s", "g", new StreamEntryID("1-1"));
    }

    @Test
    public void xpendingIdleMapsDeliveryCounts() {
        Jedis jedis = mock(Jedis.class);
        StreamPendingEntry pending = new StreamPendingEntry(new StreamEntryID("2-1"), "c1", 90000L, 3L);
        when(jedis.xpending(eq("s"), eq("g"), any(XPendingParams.class))).thenReturn(Collections.singletonList(pending));
        StandaloneRedisConnection conn = connectionWithJedis(jedis);

        List<RedisStreamPendingMessage> result = conn.xpendingIdle("s", "g", 60000L, 100);

        assertEquals(1, result.size());
        assertEquals("2-1", result.get(0).getId());
        assertEquals(3L, result.get(0).getDeliveryCount());
    }

    @Test
    public void xclaimReturnsClaimedMessage() {
        Jedis jedis = mock(Jedis.class);
        StreamEntry entry = new StreamEntry(new StreamEntryID("3-1"), Collections.singletonMap("value", "v3"));
        when(jedis.xclaim(eq("s"), eq("g"), eq("c"), eq(0L), any(XClaimParams.class), eq(new StreamEntryID("3-1"))))
                .thenReturn(Collections.singletonList(entry));
        StandaloneRedisConnection conn = connectionWithJedis(jedis);

        RedisStreamMessage claimed = conn.xclaim("s", "g", "c", "3-1");

        assertNotNull(claimed);
        assertEquals("3-1", claimed.getId());
        assertEquals("v3", claimed.getValue());
    }

    @Test
    public void xclaimReturnsNullWhenNoLongerPending() {
        Jedis jedis = mock(Jedis.class);
        when(jedis.xclaim(eq("s"), eq("g"), eq("c"), eq(0L), any(XClaimParams.class), eq(new StreamEntryID("3-1"))))
                .thenReturn(Collections.emptyList());
        StandaloneRedisConnection conn = connectionWithJedis(jedis);

        assertNull(conn.xclaim("s", "g", "c", "3-1"));
    }
}
