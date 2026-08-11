package com.boomi.connector.redis.connection;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.redis.authentication.AuthenticationType;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RedisConnectionConfigTest {

    private BrowseContext contextWith(String hosts) {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn(hosts);
        when(props.getBooleanProperty("useSSL", Boolean.FALSE)).thenReturn(false);
        when(props.getBooleanProperty("poolEnabled", Boolean.FALSE)).thenReturn(false);
        when(props.getLongProperty("connectionTimeout", 5L)).thenReturn(5L);
        when(props.getLongProperty("socketTimeout", 5L)).thenReturn(5L);
        when(props.getProperty("authenticationType")).thenReturn("None");
        when(props.getProperty("user")).thenReturn(null);
        when(props.getProperty("password")).thenReturn(null);
        when(props.getOAuth2Context("entraOAuth2")).thenReturn(null);
        when(props.getLongProperty("poolSize", 4L)).thenReturn(4L);
        when(props.getLongProperty("minPoolSize", 1L)).thenReturn(1L);
        when(props.getLongProperty("maxIdleTime", 60L)).thenReturn(60L);
        when(props.getLongProperty("maxWaitTime", 5L)).thenReturn(5L);
        when(props.getProperty("clusteringPolicy")).thenReturn(null); // default -> NON_CLUSTERED

        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        return ctx;
    }

    @Test
    public void testStandaloneHostParsing() {
        RedisConnectionConfig config = new RedisConnectionConfig(contextWith("myredis:6379"));
        assertEquals(ClusteringPolicy.NON_CLUSTERED, config.getClusteringPolicy());
        assertEquals("myredis", config.getHost());
        assertEquals(6379, config.getPort());
    }

    @Test
    public void testClusterNodesParsesSingleSeed() {
        RedisConnectionConfig config = new RedisConnectionConfig(contextWith("seed:6379"));
        assertEquals(1, config.getClusterNodes().size());
    }

    @Test
    public void testClusterNodesParsesMultipleSeeds() {
        RedisConnectionConfig config = new RedisConnectionConfig(contextWith("h1:6379,h2:6380,h3:6381"));
        assertEquals(3, config.getClusterNodes().size());
    }

    @Test
    public void testClusteringPolicyParsedFromProperty() {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getProperty("authenticationType")).thenReturn("None");
        when(props.getProperty("clusteringPolicy")).thenReturn("OSSClustered");
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        assertEquals(ClusteringPolicy.OSS_CLUSTERED, new RedisConnectionConfig(ctx).getClusteringPolicy());
        assertTrue(new RedisConnectionConfig(ctx).isOssCluster());
    }

    @Test
    public void testConnectionTimeoutConvertedToMilliseconds() {
        RedisConnectionConfig config = new RedisConnectionConfig(contextWith("localhost:6379"));
        assertEquals(5 * 1000, config.getConnectionTimeout());
        assertEquals(5 * 1000, config.getSocketTimeout());
    }

    @Test
    public void testAuthenticationTypeNone() {
        RedisConnectionConfig config = new RedisConnectionConfig(contextWith("localhost:6379"));
        assertEquals(AuthenticationType.NONE, config.getAuthenticationType());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyHostsThrowsException() {
        new RedisConnectionConfig(contextWith(""));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullHostsThrowsException() {
        new RedisConnectionConfig(contextWith(null));
    }

    @Test
    public void testPoolSizeReadFromPropertyWhenPoolEnabled() {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getBooleanProperty("useSSL", Boolean.FALSE)).thenReturn(false);
        when(props.getBooleanProperty("poolEnabled", Boolean.FALSE)).thenReturn(true);
        when(props.getLongProperty("connectionTimeout", 5L)).thenReturn(5L);
        when(props.getLongProperty("socketTimeout", 5L)).thenReturn(5L);
        when(props.getProperty("authenticationType")).thenReturn("None");
        when(props.getProperty("user")).thenReturn(null);
        when(props.getProperty("password")).thenReturn(null);
        when(props.getOAuth2Context("entraOAuth2")).thenReturn(null);
        when(props.getLongProperty("poolSize", 4L)).thenReturn(8L);
        when(props.getLongProperty("minPoolSize", 1L)).thenReturn(2L);
        when(props.getLongProperty("maxIdleTime", 60L)).thenReturn(60L);
        when(props.getLongProperty("maxWaitTime", 5L)).thenReturn(5L);

        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);

        RedisConnectionConfig config = new RedisConnectionConfig(ctx);
        assertTrue(config.isPoolEnabled());
        assertEquals(8, config.getPoolSize());
        assertEquals(2, config.getMinPoolSize());
    }

    @Test
    public void testPoolSizeIsZeroWhenPoolDisabled() {
        RedisConnectionConfig config = new RedisConnectionConfig(contextWith("localhost:6379"));
        assertFalse(config.isPoolEnabled());
        assertEquals(0, config.getPoolSize());
    }

    @Test
    public void clusterMaxTotalIsOneWhenPoolingDisabled() {
        // A disabled-pooling cluster client is unshared and rebuilt fresh per execution (see
        // ClusteredRedisConnection), so it is only ever used by one execution at a time.
        RedisConnectionConfig config = new RedisConnectionConfig(contextWith("localhost:6379"));
        assertFalse(config.isPoolEnabled());
        assertEquals(1, config.getClusterMaxTotal());
    }

    @Test
    public void clusterMaxTotalUsesConfiguredSizeWhenPoolingEnabled() {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getProperty("authenticationType")).thenReturn("None");
        when(props.getProperty("clusteringPolicy")).thenReturn("OSSClustered");
        when(props.getBooleanProperty("poolEnabled", Boolean.FALSE)).thenReturn(true);
        when(props.getLongProperty("poolSize", 4L)).thenReturn(6L);
        when(props.getLongProperty("minPoolSize", 1L)).thenReturn(1L);
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        assertEquals(6, new RedisConnectionConfig(ctx).getClusterMaxTotal());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidHostFormatThrowsOnGetHost() {
        RedisConnectionConfig config = new RedisConnectionConfig(contextWith("localhostnoport"));
        config.getHost();
    }

    @Test
    public void connectionIdReadFromIdProperty() {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getProperty("authenticationType")).thenReturn("None");
        when(props.getProperty("id")).thenReturn("abc-123-component");
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        assertEquals("abc-123-component", new RedisConnectionConfig(ctx).getConnectionId());
    }

    @Test
    public void connectionIdDefaultsToEmptyWhenAbsent() {
        // Mocked PropertyMaps (and possibly some runtime paths) have no "id" entry; the config
        // must normalize to "" so RedisClientSettings equality never trips over null.
        RedisConnectionConfig config = new RedisConnectionConfig(contextWith("localhost:6379"));
        assertEquals("", config.getConnectionId());
    }
}
