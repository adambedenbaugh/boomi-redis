package com.boomi.connector.redis.connection;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import org.junit.Test;
import redis.clients.jedis.*;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ClusteredRedisConnectionQueueTest {

    private RedisConnectionConfig clusterConfig() {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:7000");
        when(props.getProperty("authenticationType")).thenReturn("None");
        when(props.getProperty("clusteringPolicy")).thenReturn("OSSClustered");
        when(props.getBooleanProperty("poolEnabled", Boolean.FALSE)).thenReturn(false);
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        return new RedisConnectionConfig(ctx);
    }

    private ClusteredRedisConnection connectionWithCluster(JedisCluster cluster) {
        JedisClientFactory factory = mock(JedisClientFactory.class);
        when(factory.createCluster(any(Set.class), any(JedisClientConfig.class), anyInt(),
                any(java.time.Duration.class), any())).thenReturn(cluster);
        return new ClusteredRedisConnection(clusterConfig(), factory);
    }

    @Test
    public void rpushDelegatesToCluster() {
        JedisCluster cluster = mock(JedisCluster.class);
        ClusteredRedisConnection conn = connectionWithCluster(cluster);
        conn.rpush("q", "v");
        verify(cluster).rpush("q", "v");
    }

    @Test
    public void blpopDelegatesAndReturnsValue() {
        JedisCluster cluster = mock(JedisCluster.class);
        when(cluster.blpop(5, "q")).thenReturn(java.util.Arrays.asList("q", "v"));
        ClusteredRedisConnection conn = connectionWithCluster(cluster);
        assertEquals("v", conn.blpop("q", 5));
    }

    @Test
    public void xgroupCreateSwallowsBusygroup() {
        JedisCluster cluster = mock(JedisCluster.class);
        when(cluster.xgroupCreate(eq("s"), eq("g"), any(StreamEntryID.class), eq(true)))
                .thenThrow(new redis.clients.jedis.exceptions.JedisDataException("BUSYGROUP already exists"));
        ClusteredRedisConnection conn = connectionWithCluster(cluster);
        conn.xgroupCreate("s", "g");
    }

    @Test
    public void xaddSendsValueField() {
        JedisCluster cluster = mock(JedisCluster.class);
        ClusteredRedisConnection conn = connectionWithCluster(cluster);
        conn.xadd("s", "hello");
        verify(cluster).xadd(eq("s"), eq(StreamEntryID.NEW_ENTRY), eq(Collections.singletonMap("value", "hello")));
    }

    @Test
    public void xackAcknowledgesById() {
        JedisCluster cluster = mock(JedisCluster.class);
        ClusteredRedisConnection conn = connectionWithCluster(cluster);
        conn.xack("s", "g", "1-1");
        verify(cluster).xack("s", "g", new StreamEntryID("1-1"));
    }

    @Test
    public void xclaimReturnsNullWhenNoLongerPending() {
        JedisCluster cluster = mock(JedisCluster.class);
        when(cluster.xclaim(eq("s"), eq("g"), eq("c"), eq(0L), any(), eq(new StreamEntryID("3-1"))))
                .thenReturn(Collections.emptyList());
        ClusteredRedisConnection conn = connectionWithCluster(cluster);
        assertNull(conn.xclaim("s", "g", "c", "3-1"));
    }
}
