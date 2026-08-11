package com.boomi.connector.redis.connection;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.*;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ClusteredRedisConnectionTest {

    @Before
    public void resetSharedClustersBefore() {
        ClusteredRedisConnection.closeAllSharedClusters();
    }

    @After
    public void resetSharedClustersAfter() {
        ClusteredRedisConnection.closeAllSharedClusters();
    }

    private RedisConnectionConfig clusterConfig() {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("node1:7000,node2:7001");
        when(props.getProperty("authenticationType")).thenReturn("None");
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        return new RedisConnectionConfig(ctx);
    }

    private RedisConnectionConfig pooledClusterConfig() {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("node1:7000,node2:7001");
        when(props.getProperty("authenticationType")).thenReturn("None");
        when(props.getBooleanProperty("poolEnabled", Boolean.FALSE)).thenReturn(true);
        when(props.getLongProperty("poolSize", 4L)).thenReturn(5L);
        when(props.getLongProperty("minPoolSize", 1L)).thenReturn(1L);
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        return new RedisConnectionConfig(ctx);
    }

    private RedisConnectionConfig pooledClusterConfigWithBasicAuth(String password) {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("node1:7000,node2:7001");
        when(props.getProperty("authenticationType")).thenReturn("Basic");
        when(props.getProperty("user")).thenReturn("alice");
        when(props.getProperty("password")).thenReturn(password);
        when(props.getBooleanProperty("poolEnabled", Boolean.FALSE)).thenReturn(true);
        when(props.getLongProperty("poolSize", 4L)).thenReturn(5L);
        when(props.getLongProperty("minPoolSize", 1L)).thenReturn(1L);
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
    public void poolingDisabledBuildsAFreshUnsharedClusterEveryTimeAndClosesItImmediately() {
        JedisCluster clusterA = mock(JedisCluster.class);
        JedisCluster clusterB = mock(JedisCluster.class);
        JedisClientFactory factory = mock(JedisClientFactory.class);
        when(factory.createCluster(anySet(), any(JedisClientConfig.class), anyInt(),
                any(Duration.class), any(GenericObjectPoolConfig.class))).thenReturn(clusterA, clusterB);

        ClusteredRedisConnection a = new ClusteredRedisConnection(clusterConfig(), factory);
        a.close();
        ClusteredRedisConnection b = new ClusteredRedisConnection(clusterConfig(), factory);
        b.close();

        verify(factory, times(2)).createCluster(anySet(), any(), anyInt(), any(), any());
        verify(clusterA, times(1)).close();
        verify(clusterB, times(1)).close();
        assertEquals(0, ClusteredRedisConnection.getSharedClusterCount());
    }

    @Test
    public void poolingEnabledCreatesClusterOnceAndReusesByKey() {
        JedisCluster cluster = mock(JedisCluster.class);
        JedisClientFactory factory = mock(JedisClientFactory.class);
        when(factory.createCluster(anySet(), any(JedisClientConfig.class), anyInt(),
                any(Duration.class), any(GenericObjectPoolConfig.class))).thenReturn(cluster);

        ClusteredRedisConnection a = new ClusteredRedisConnection(pooledClusterConfig(), factory);
        ClusteredRedisConnection b = new ClusteredRedisConnection(pooledClusterConfig(), factory);

        verify(factory, times(1)).createCluster(anySet(), any(), anyInt(), any(), any());
        assertEquals(1, ClusteredRedisConnection.getSharedClusterCount());
        a.close();
        b.close();
    }

    @Test
    public void poolingEnabledSequentialExecutionsReuseTheSameSharedClusterAndNeverCloseIt() {
        // Regression test: an execution closing its connection must not tear down the shared
        // cluster client out from under the next sequential execution (mirrors the standalone
        // pooled-connection fix - see StandalonePooledRedisConnectionTest).
        JedisCluster cluster = mock(JedisCluster.class);
        JedisClientFactory factory = mock(JedisClientFactory.class);
        when(factory.createCluster(anySet(), any(JedisClientConfig.class), anyInt(),
                any(Duration.class), any(GenericObjectPoolConfig.class))).thenReturn(cluster);

        ClusteredRedisConnection a = new ClusteredRedisConnection(pooledClusterConfig(), factory);
        a.close();
        ClusteredRedisConnection b = new ClusteredRedisConnection(pooledClusterConfig(), factory);
        b.close();

        verify(factory, times(1)).createCluster(anySet(), any(), anyInt(), any(), any());
        verify(cluster, never()).close();
        assertEquals(1, ClusteredRedisConnection.getSharedClusterCount());
    }

    @Test
    public void credentialChangeEvictsTheSupersededIdleClusterForTheSameNodeSet() {
        JedisCluster oldCluster = mock(JedisCluster.class);
        JedisClientFactory factory = mock(JedisClientFactory.class);
        when(factory.createCluster(anySet(), any(JedisClientConfig.class), anyInt(),
                any(Duration.class), any(GenericObjectPoolConfig.class))).thenReturn(oldCluster);
        ClusteredRedisConnection a = new ClusteredRedisConnection(pooledClusterConfigWithBasicAuth("pw"), factory);
        a.close(); // no in-flight execution holds the old client

        JedisCluster rotatedCluster = mock(JedisCluster.class);
        when(factory.createCluster(anySet(), any(JedisClientConfig.class), anyInt(),
                any(Duration.class), any(GenericObjectPoolConfig.class))).thenReturn(rotatedCluster);
        ClusteredRedisConnection b = new ClusteredRedisConnection(pooledClusterConfigWithBasicAuth("rotated-pw"), factory);

        verify(oldCluster, times(1)).close();
        verify(rotatedCluster, never()).close();
        assertEquals(1, ClusteredRedisConnection.getSharedClusterCount());
        b.close();
    }

    @Test
    public void credentialChangeDoesNotEvictAClusterStillHeldByAnInFlightExecution() {
        JedisCluster oldCluster = mock(JedisCluster.class);
        JedisClientFactory factory = mock(JedisClientFactory.class);
        when(factory.createCluster(anySet(), any(JedisClientConfig.class), anyInt(),
                any(Duration.class), any(GenericObjectPoolConfig.class))).thenReturn(oldCluster);
        ClusteredRedisConnection a = new ClusteredRedisConnection(pooledClusterConfigWithBasicAuth("pw"), factory);
        // a stays open: an in-flight execution still holds the old client

        JedisCluster rotatedCluster = mock(JedisCluster.class);
        when(factory.createCluster(anySet(), any(JedisClientConfig.class), anyInt(),
                any(Duration.class), any(GenericObjectPoolConfig.class))).thenReturn(rotatedCluster);
        ClusteredRedisConnection b = new ClusteredRedisConnection(pooledClusterConfigWithBasicAuth("rotated-pw"), factory);

        verify(oldCluster, never()).close();
        assertEquals(2, ClusteredRedisConnection.getSharedClusterCount());
        a.close();
        b.close();
    }

    @Test
    public void closeAllSharedClustersClosesAClusterEvenAfterEveryInstanceAlreadyClosed() {
        JedisCluster cluster = mock(JedisCluster.class);
        JedisClientFactory factory = mock(JedisClientFactory.class);
        when(factory.createCluster(anySet(), any(JedisClientConfig.class), anyInt(),
                any(Duration.class), any(GenericObjectPoolConfig.class))).thenReturn(cluster);

        ClusteredRedisConnection a = new ClusteredRedisConnection(pooledClusterConfig(), factory);
        a.close();

        ClusteredRedisConnection.closeAllSharedClusters();

        verify(cluster, times(1)).close();
        assertEquals(0, ClusteredRedisConnection.getSharedClusterCount());
    }

    @Test
    public void closeDoesNotAffectAnUnrelatedClusterRegisteredUnderTheSameKeyAfterReplacement() {
        JedisCluster clusterA = mock(JedisCluster.class);
        JedisClientFactory factory = mock(JedisClientFactory.class);
        when(factory.createCluster(anySet(), any(JedisClientConfig.class), anyInt(),
                any(Duration.class), any(GenericObjectPoolConfig.class))).thenReturn(clusterA);
        ClusteredRedisConnection a = new ClusteredRedisConnection(pooledClusterConfig(), factory);

        // Simulate closeAllSharedClusters() running (e.g. shutdown) while `a` still holds a
        // reference to the now-discarded original client, followed by a new instance registering a
        // fresh client under the identical key.
        ClusteredRedisConnection.closeAllSharedClusters();
        JedisCluster clusterB = mock(JedisCluster.class);
        when(factory.createCluster(anySet(), any(JedisClientConfig.class), anyInt(),
                any(Duration.class), any(GenericObjectPoolConfig.class))).thenReturn(clusterB);
        ClusteredRedisConnection b = new ClusteredRedisConnection(pooledClusterConfig(), factory);

        a.close();

        verify(clusterB, never()).close();
        assertEquals(1, ClusteredRedisConnection.getSharedClusterCount());
        b.close();
    }

    @Test
    public void delAllScansEachNodeThenPipelinesDeletesNeverClusterScanOrMultiKeyDel() {
        JedisCluster cluster = mock(JedisCluster.class);
        ConnectionPool pool = mock(ConnectionPool.class);
        Connection conn = mock(Connection.class);
        when(pool.getResource()).thenReturn(conn);
        Map<String, ConnectionPool> nodes = new HashMap<>();
        nodes.put("127.0.0.1:7000", pool);
        when(cluster.getClusterNodes()).thenReturn(nodes);

        Jedis node = mock(Jedis.class);
        when(node.scan(eq(ScanParams.SCAN_POINTER_START), any(ScanParams.class)))
                .thenReturn(new ScanResult<>(ScanParams.SCAN_POINTER_START, Arrays.asList("a", "b")));

        ClusterPipeline pipeline = mock(ClusterPipeline.class);
        when(cluster.pipelined()).thenReturn(pipeline);

        JedisClientFactory factory = mock(JedisClientFactory.class);
        when(factory.createCluster(anySet(), any(JedisClientConfig.class), anyInt(),
                any(Duration.class), any(GenericObjectPoolConfig.class))).thenReturn(cluster);
        when(factory.createClientFromConnection(conn)).thenReturn(node);

        ClusteredRedisConnection c = new ClusteredRedisConnection(clusterConfig(), factory);
        c.delAll("prefix:");

        verify(node).scan(eq(ScanParams.SCAN_POINTER_START), any(ScanParams.class));
        verify(pipeline).del("a");
        verify(pipeline).del("b");
        verify(pipeline).sync();
        // close() is what returns the pipeline's borrowed per-node connections to the pools;
        // sync() alone leaks them.
        verify(pipeline).close();
        verify(cluster, never()).del(anyString());
        verify(cluster, never()).del(any(String[].class));
        verify(cluster, never()).scan(anyString(), any(ScanParams.class));
        c.close();
    }

    @Test
    public void getAllScansEachNodeThenPipelinesGetsNeverClusterScan() {
        JedisCluster cluster = mock(JedisCluster.class);
        ConnectionPool pool = mock(ConnectionPool.class);
        Connection conn = mock(Connection.class);
        when(pool.getResource()).thenReturn(conn);
        Map<String, ConnectionPool> nodes = new HashMap<>();
        nodes.put("127.0.0.1:7000", pool);
        when(cluster.getClusterNodes()).thenReturn(nodes);

        Jedis node = mock(Jedis.class);
        when(node.scan(eq(ScanParams.SCAN_POINTER_START), any(ScanParams.class)))
                .thenReturn(new ScanResult<>(ScanParams.SCAN_POINTER_START, Arrays.asList("a", "b")));

        ClusterPipeline pipeline = mock(ClusterPipeline.class);
        when(cluster.pipelined()).thenReturn(pipeline);
        Response<String> responseA = mock(Response.class);
        when(responseA.get()).thenReturn("1");
        Response<String> responseB = mock(Response.class);
        when(responseB.get()).thenReturn("2");
        when(pipeline.get("a")).thenReturn(responseA);
        when(pipeline.get("b")).thenReturn(responseB);

        JedisClientFactory factory = mock(JedisClientFactory.class);
        when(factory.createCluster(anySet(), any(JedisClientConfig.class), anyInt(),
                any(Duration.class), any(GenericObjectPoolConfig.class))).thenReturn(cluster);
        when(factory.createClientFromConnection(conn)).thenReturn(node);

        ClusteredRedisConnection c = new ClusteredRedisConnection(clusterConfig(), factory);
        Map<String, String> result = c.getAll("prefix:");

        assertEquals(2, result.size());
        assertEquals("1", result.get("a"));
        assertEquals("2", result.get("b"));
        verify(pipeline).sync();
        // close() is what returns the pipeline's borrowed per-node connections to the pools;
        // sync() alone leaks them.
        verify(pipeline).close();
        verify(cluster, never()).get(anyString());
        verify(cluster, never()).scan(anyString(), any(ScanParams.class));
        c.close();
    }
}
