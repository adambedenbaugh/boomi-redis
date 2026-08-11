package com.boomi.connector.redis.connection;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.redis.pool.RedisClientPoolManager;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.*;

import java.time.Duration;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RedisConnectionFactoryTest {

    private JedisClientFactory factory;

    @Before
    public void setUp() {
        RedisClientPoolManager.closeAll();
        ClusteredRedisConnection.closeAllSharedClusters();
        factory = mock(JedisClientFactory.class);
        when(factory.createClient(any(HostAndPort.class), any(JedisClientConfig.class)))
                .thenReturn(mock(Jedis.class));
        JedisPool pool = mock(JedisPool.class);
        when(pool.isClosed()).thenReturn(false);
        when(factory.createPool(any(GenericObjectPoolConfig.class), any(HostAndPort.class),
                any(JedisClientConfig.class))).thenReturn(pool);
        when(factory.createCluster(anySet(), any(JedisClientConfig.class), anyInt(),
                any(Duration.class), any(GenericObjectPoolConfig.class))).thenReturn(mock(JedisCluster.class));
    }

    @After
    public void tearDown() {
        RedisClientPoolManager.closeAll();
        ClusteredRedisConnection.closeAllSharedClusters();
    }

    private RedisConnectionConfig config(String policy, boolean poolEnabled) {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getProperty("authenticationType")).thenReturn("None");
        when(props.getProperty("clusteringPolicy")).thenReturn(policy);
        when(props.getBooleanProperty("poolEnabled", Boolean.FALSE)).thenReturn(poolEnabled);
        when(props.getLongProperty("poolSize", 4L)).thenReturn(4L);
        when(props.getLongProperty("minPoolSize", 1L)).thenReturn(1L);
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        return new RedisConnectionConfig(ctx);
    }

    @Test
    public void ossRoutesToCluster() {
        assertTrue(RedisConnectionFactory.createConnection(config("OSSClustered", false), factory)
                instanceof ClusteredRedisConnection);
    }

    @Test
    public void nonClusteredNoPoolRoutesToStandalone() {
        assertTrue(RedisConnectionFactory.createConnection(config("NonClustered", false), factory)
                instanceof StandaloneRedisConnection);
    }

    @Test
    public void nonClusteredWithPoolRoutesToPooled() {
        assertTrue(RedisConnectionFactory.createConnection(config("NonClustered", true), factory)
                instanceof StandalonePooledRedisConnection);
    }

    @Test
    public void enterpriseNoPoolRoutesToStandalone() {
        assertTrue(RedisConnectionFactory.createConnection(config("EnterpriseClustered", false), factory)
                instanceof StandaloneRedisConnection);
    }

    @Test
    public void enterpriseWithPoolRoutesToPooled() {
        assertTrue(RedisConnectionFactory.createConnection(config("EnterpriseClustered", true), factory)
                instanceof StandalonePooledRedisConnection);
    }
}
