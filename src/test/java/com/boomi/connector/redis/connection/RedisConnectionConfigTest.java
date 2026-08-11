package com.boomi.connector.redis.connection;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.redis.authentication.AuthenticationType;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotEquals;
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
    public void clusterMaxTotalDefaultsWhenPoolingDisabled() {
        RedisConnectionConfig config = new RedisConnectionConfig(contextWith("localhost:6379"));
        assertFalse(config.isPoolEnabled());
        assertEquals(8, config.getClusterMaxTotal());
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
    public void authIdentityNoneWhenNoAuth() {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getProperty("authenticationType")).thenReturn("None");
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        assertEquals("none", new RedisConnectionConfig(ctx).getAuthIdentity());
    }

    /** Builds an Entra config whose OAuth2Context returns the given client id / secret / token URL. */
    private static RedisConnectionConfig entraConfig(String clientId, String clientSecret, String accessTokenUrl) {
        com.boomi.connector.api.OAuth2Context oauth = mock(com.boomi.connector.api.OAuth2Context.class);
        when(oauth.getClientId()).thenReturn(clientId);
        when(oauth.getClientSecret()).thenReturn(clientSecret);
        when(oauth.getAccessTokenUrl()).thenReturn(accessTokenUrl);
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getProperty("authenticationType")).thenReturn("MicrosoftEntraClientSecretCredential");
        when(props.getOAuth2Context("entraOAuth2")).thenReturn(oauth);
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        return new RedisConnectionConfig(ctx);
    }

    @Test
    public void authIdentityUsesClientIdForEntra() {
        String identity = entraConfig("client-abc", "secret", "https://login.microsoftonline.com/t/oauth2/v2.0/token")
                .getAuthIdentity();
        assertTrue(identity.startsWith("entra:client-abc:"));
    }

    @Test
    public void authIdentityEntraExcludesRotatingToken() {
        // The identity is derived only from stable config (client id/secret/token URL); the rotating
        // access token is never part of it, so token refresh cannot change the pool key.
        String secret = "the-client-secret";
        String url = "https://login.microsoftonline.com/t/oauth2/v2.0/token";
        String identity = entraConfig("client-abc", secret, url).getAuthIdentity();
        assertFalse("identity must not embed the rotating access token", identity.contains("access_token"));
        // Stable inputs -> stable identity (a second identical config yields the same key).
        assertEquals(identity, entraConfig("client-abc", secret, url).getAuthIdentity());
    }

    @Test
    public void authIdentityEntraDistinguishesClientSecret() {
        // Fixing a bad client secret must change the pool key so new executions get a fresh pool.
        String url = "https://login.microsoftonline.com/t/oauth2/v2.0/token";
        String wrongSecret = entraConfig("client-abc", "wrong-secret-id", url).getAuthIdentity();
        String rightSecret = entraConfig("client-abc", "correct-secret-value", url).getAuthIdentity();
        assertNotEquals(wrongSecret, rightSecret);
    }

    @Test
    public void authIdentityEntraDistinguishesAccessTokenUrl() {
        // Switching Azure Commercial -> Government (different token URL) must change the pool key.
        String commercial = entraConfig("client-abc", "secret",
                "https://login.microsoftonline.com/t/oauth2/v2.0/token").getAuthIdentity();
        String government = entraConfig("client-abc", "secret",
                "https://login.microsoftonline.us/t/oauth2/v2.0/token").getAuthIdentity();
        assertNotEquals(commercial, government);
    }

    @Test
    public void authIdentityEntraDoesNotEmitClientSecretVerbatim() {
        // The pool key is logged, so the secret must be hashed, never included in plaintext.
        String secret = "super-secret-value-123";
        String identity = entraConfig("client-abc", secret,
                "https://login.microsoftonline.com/t/oauth2/v2.0/token").getAuthIdentity();
        assertFalse("client secret must not appear verbatim in the pool key", identity.contains(secret));
    }

    @Test
    public void authIdentityBasicDistinguishesPassword() {
        PropertyMap props1 = mock(PropertyMap.class);
        when(props1.getProperty("hosts")).thenReturn("localhost:6379");
        when(props1.getProperty("authenticationType")).thenReturn("Basic");
        when(props1.getProperty("user")).thenReturn("someuser");
        when(props1.getProperty("password")).thenReturn("password1");
        BrowseContext ctx1 = mock(BrowseContext.class);
        when(ctx1.getConnectionProperties()).thenReturn(props1);

        PropertyMap props2 = mock(PropertyMap.class);
        when(props2.getProperty("hosts")).thenReturn("localhost:6379");
        when(props2.getProperty("authenticationType")).thenReturn("Basic");
        when(props2.getProperty("user")).thenReturn("someuser");
        when(props2.getProperty("password")).thenReturn("password2");
        BrowseContext ctx2 = mock(BrowseContext.class);
        when(ctx2.getConnectionProperties()).thenReturn(props2);

        String identity1 = new RedisConnectionConfig(ctx1).getAuthIdentity();
        String identity2 = new RedisConnectionConfig(ctx2).getAuthIdentity();
        assertNotEquals(identity1, identity2);
    }
}
