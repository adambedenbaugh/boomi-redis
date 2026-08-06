package com.boomi.connector.redis.connection;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Test;
import redis.clients.jedis.*;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.time.Duration;
import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ClusteredRedisConnectionTest {

    private RedisConnectionConfig clusterConfig() {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("node1:7000,node2:7001");
        when(props.getProperty("authenticationType")).thenReturn("None");
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        return new RedisConnectionConfig(ctx);
    }

    @Test
    public void buildsClusterViaFactoryAndDelegatesGet() {
        JedisCluster cluster = mock(JedisCluster.class);
        when(cluster.get("k")).thenReturn("v");
        JedisClientFactory factory = mock(JedisClientFactory.class);
        when(factory.createCluster(anySet(), any(JedisClientConfig.class), anyInt(),
                any(Duration.class), any(GenericObjectPoolConfig.class))).thenReturn(cluster);

        ClusteredRedisConnection conn = new ClusteredRedisConnection(clusterConfig(), factory);
        assertEquals("v", conn.get("k"));
        verify(factory, times(1)).createCluster(anySet(), any(), anyInt(), any(), any());
        conn.close();
    }

    @Test
    public void delAllDeletesOneKeyPerCallNeverMultiKey() {
        JedisCluster cluster = mock(JedisCluster.class);
        when(cluster.scan(eq(ScanParams.SCAN_POINTER_START), any(ScanParams.class)))
                .thenReturn(new ScanResult<>(ScanParams.SCAN_POINTER_START, Arrays.asList("a", "b")));
        JedisClientFactory factory = mock(JedisClientFactory.class);
        when(factory.createCluster(anySet(), any(JedisClientConfig.class), anyInt(),
                any(Duration.class), any(GenericObjectPoolConfig.class))).thenReturn(cluster);

        ClusteredRedisConnection conn = new ClusteredRedisConnection(clusterConfig(), factory);
        conn.delAll("prefix:");

        verify(cluster).del("a");
        verify(cluster).del("b");
        verify(cluster, never()).del(any(String[].class));
        conn.close();
    }
}
