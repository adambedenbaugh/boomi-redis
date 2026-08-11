package com.boomi.connector.redis.pool;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.redis.connection.RedisConnectionConfig;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RedisClientSettingsTest {

    /** Builds a config from a fully-stubbed PropertyMap; individual tests override single stubs. */
    private static PropertyMap baseProps() {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getBooleanProperty("useSSL", Boolean.FALSE)).thenReturn(false);
        when(props.getBooleanProperty("poolEnabled", Boolean.FALSE)).thenReturn(true);
        when(props.getLongProperty("connectionTimeout", 5L)).thenReturn(5L);
        when(props.getLongProperty("socketTimeout", 5L)).thenReturn(5L);
        when(props.getProperty("authenticationType")).thenReturn("Basic");
        when(props.getProperty("user")).thenReturn("alice");
        when(props.getProperty("password")).thenReturn("pw-1");
        when(props.getOAuth2Context("entraOAuth2")).thenReturn(null);
        when(props.getLongProperty("poolSize", 4L)).thenReturn(4L);
        when(props.getLongProperty("minPoolSize", 1L)).thenReturn(1L);
        when(props.getLongProperty("maxIdleTime", 60L)).thenReturn(60L);
        when(props.getLongProperty("maxWaitTime", 5L)).thenReturn(5L);
        when(props.getProperty("clusteringPolicy")).thenReturn("NonClustered");
        return props;
    }

    private static RedisClientSettings settings(PropertyMap props) {
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        return new RedisClientSettings(new RedisConnectionConfig(ctx));
    }

    private static OAuth2Context entraContext(String clientId, String secret, String tokenUrl) {
        OAuth2Context oauth = mock(OAuth2Context.class);
        when(oauth.getClientId()).thenReturn(clientId);
        when(oauth.getClientSecret()).thenReturn(secret);
        when(oauth.getAccessTokenUrl()).thenReturn(tokenUrl);
        return oauth;
    }

    private static PropertyMap entraProps(OAuth2Context oauth) {
        PropertyMap props = baseProps();
        when(props.getProperty("authenticationType")).thenReturn("MicrosoftEntraClientSecretCredential");
        when(props.getOAuth2Context("entraOAuth2")).thenReturn(oauth);
        return props;
    }

    @Test
    public void identicalConfigurationsAreEqualWithEqualHashCodes() {
        RedisClientSettings a = settings(baseProps());
        RedisClientSettings b = settings(baseProps());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void passwordChangeChangesEquality() {
        PropertyMap rotated = baseProps();
        when(rotated.getProperty("password")).thenReturn("pw-2");
        assertNotEquals(settings(baseProps()), settings(rotated));
    }

    @Test
    public void basicCredentialsIgnoredWhenAuthTypeIsNone() {
        // Mirrors AdapterSettings: credentials participate in equality only when auth uses them.
        PropertyMap a = baseProps();
        when(a.getProperty("authenticationType")).thenReturn("None");
        when(a.getProperty("user")).thenReturn("alice");
        PropertyMap b = baseProps();
        when(b.getProperty("authenticationType")).thenReturn("None");
        when(b.getProperty("user")).thenReturn("bob");
        assertEquals(settings(a), settings(b));
    }

    @Test
    public void rotatingEntraTokenDoesNotAffectEquality() {
        // Two OAuth2Context INSTANCES (fresh per execution at runtime) with the same stable
        // credential fields must produce equal settings - otherwise pooling silently never reuses.
        RedisClientSettings a = settings(entraProps(entraContext("client-1", "secret-1", "https://login/token")));
        RedisClientSettings b = settings(entraProps(entraContext("client-1", "secret-1", "https://login/token")));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void entraClientSecretChangeChangesEquality() {
        RedisClientSettings a = settings(entraProps(entraContext("client-1", "secret-1", "https://login/token")));
        RedisClientSettings b = settings(entraProps(entraContext("client-1", "secret-2", "https://login/token")));
        assertNotEquals(a, b);
    }

    @Test
    public void entraAccessTokenUrlChangeChangesEquality() {
        // Commercial -> Government cutover must rebuild the pool.
        RedisClientSettings a = settings(entraProps(entraContext("client-1", "secret-1",
                "https://login.microsoftonline.com/t/oauth2/v2.0/token")));
        RedisClientSettings b = settings(entraProps(entraContext("client-1", "secret-1",
                "https://login.microsoftonline.us/t/oauth2/v2.0/token")));
        assertNotEquals(a, b);
    }

    @Test
    public void entraContextWithNullFieldsIsHandled() {
        // A misconfigured OAuth2 component must not NPE settings construction.
        RedisClientSettings a = settings(entraProps(entraContext(null, null, null)));
        RedisClientSettings b = settings(entraProps(entraContext(null, null, null)));
        assertEquals(a, b);
    }

    @Test
    public void poolFieldChangesChangeEquality() {
        PropertyMap resized = baseProps();
        when(resized.getLongProperty("poolSize", 4L)).thenReturn(8L);
        assertNotEquals(settings(baseProps()), settings(resized));

        PropertyMap retimed = baseProps();
        when(retimed.getLongProperty("connectionTimeout", 5L)).thenReturn(30L);
        assertNotEquals(settings(baseProps()), settings(retimed));
    }

    @Test
    public void clusteringPolicyChangeChangesEquality() {
        PropertyMap clustered = baseProps();
        when(clustered.getProperty("clusteringPolicy")).thenReturn("OSSClustered");
        assertNotEquals(settings(baseProps()), settings(clustered));
    }

    @Test
    public void toStringNeverContainsSecrets() {
        RedisClientSettings basic = settings(baseProps());
        assertFalse("password must be redacted", basic.toString().contains("pw-1"));

        RedisClientSettings entra = settings(entraProps(entraContext("client-1", "secret-1", "https://login/token")));
        assertFalse("client secret must be redacted", entra.toString().contains("secret-1"));
    }

    @Test
    public void isPoolEnabledExposed() {
        assertTrue(settings(baseProps()).isPoolEnabled());
    }
}
