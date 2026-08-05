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
        when(props.getBooleanProperty("useSSL")).thenReturn(false);
        when(props.getBooleanProperty("poolEnabled")).thenReturn(false);
        when(props.getLongProperty("connectionTimeout", 30L)).thenReturn(30L);
        when(props.getLongProperty("socketTimeout", 30L)).thenReturn(30L);
        when(props.getProperty("authenticationType")).thenReturn("None");
        when(props.getProperty("user")).thenReturn(null);
        when(props.getProperty("password")).thenReturn(null);
        when(props.getOAuth2Context("entraOAuth2")).thenReturn(null);
        when(props.getLongProperty("poolSize", 4L)).thenReturn(4L);
        when(props.getLongProperty("minPoolSize", 1L)).thenReturn(1L);
        when(props.getLongProperty("maxIdleTime", 60L)).thenReturn(60L);
        when(props.getLongProperty("maxWaitTime", 60L)).thenReturn(60L);

        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        return ctx;
    }

    @Test
    public void testStandaloneHostParsing() {
        RedisConnectionConfig config = new RedisConnectionConfig(contextWith("myredis:6379"));
        assertFalse(config.isCluster());
        assertEquals("myredis", config.getHost());
        assertEquals(6379, config.getPort());
    }

    @Test
    public void testClusterHostParsing() {
        RedisConnectionConfig config = new RedisConnectionConfig(contextWith("h1:6379,h2:6380,h3:6381"));
        assertTrue(config.isCluster());
        assertEquals(3, config.getClusterNodes().size());
    }

    @Test
    public void testConnectionTimeoutConvertedToMilliseconds() {
        RedisConnectionConfig config = new RedisConnectionConfig(contextWith("localhost:6379"));
        assertEquals(30 * 1000, config.getConnectionTimeout());
        assertEquals(30 * 1000, config.getSocketTimeout());
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
        when(props.getBooleanProperty("useSSL")).thenReturn(false);
        when(props.getBooleanProperty("poolEnabled")).thenReturn(true);
        when(props.getLongProperty("connectionTimeout", 30L)).thenReturn(30L);
        when(props.getLongProperty("socketTimeout", 30L)).thenReturn(30L);
        when(props.getProperty("authenticationType")).thenReturn("None");
        when(props.getProperty("user")).thenReturn(null);
        when(props.getProperty("password")).thenReturn(null);
        when(props.getOAuth2Context("entraOAuth2")).thenReturn(null);
        when(props.getLongProperty("poolSize", 4L)).thenReturn(8L);
        when(props.getLongProperty("minPoolSize", 1L)).thenReturn(2L);
        when(props.getLongProperty("maxIdleTime", 60L)).thenReturn(60L);
        when(props.getLongProperty("maxWaitTime", 60L)).thenReturn(60L);

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

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidHostFormatThrowsOnGetHost() {
        RedisConnectionConfig config = new RedisConnectionConfig(contextWith("localhostnoport"));
        config.getHost();
    }

    @Test
    public void authIdentityNoneWhenNoAuth() {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getProperty("authenticationType")).thenReturn("None");
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        assertEquals("none", new RedisConnectionConfig(ctx).getAuthIdentity());
    }

    @Test
    public void authIdentityUsesClientIdForEntraNotToken() {
        com.boomi.connector.api.OAuth2Context oauth = mock(com.boomi.connector.api.OAuth2Context.class);
        when(oauth.getClientId()).thenReturn("client-abc");
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getProperty("authenticationType")).thenReturn("MicrosoftEntraClientSecretCredential");
        when(props.getOAuth2Context("entraOAuth2")).thenReturn(oauth);
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        assertEquals("entra:client-abc", new RedisConnectionConfig(ctx).getAuthIdentity());
    }
}
