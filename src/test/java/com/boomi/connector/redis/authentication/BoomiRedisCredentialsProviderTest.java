package com.boomi.connector.redis.authentication;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.OAuth2Token;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.RedisCredentials;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class BoomiRedisCredentialsProviderTest {

    @Before
    public void clearRegistryBefore() {
        BoomiRedisCredentialsProvider.clearCurrentContexts();
    }

    @After
    public void clearRegistryAfter() {
        BoomiRedisCredentialsProvider.clearCurrentContexts();
    }

    /** Base64url, no padding — matches the connector's Base64.getUrlDecoder(). */
    private static String jwtWithOid(String oid) {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String header = enc.encodeToString("{\"alg\":\"none\"}".getBytes(StandardCharsets.UTF_8));
        String payload = enc.encodeToString(("{\"oid\":\"" + oid + "\"}").getBytes(StandardCharsets.UTF_8));
        return header + "." + payload + ".";
    }

    @Test
    public void noneReturnsNullCredentials() {
        BoomiRedisCredentialsProvider p =
                new BoomiRedisCredentialsProvider(AuthenticationType.NONE, null, null, null);
        assertNull(p.get());
    }

    @Test
    public void basicReturnsStaticUserAndPassword() {
        BoomiRedisCredentialsProvider p =
                new BoomiRedisCredentialsProvider(AuthenticationType.BASIC, "alice", "s3cret", null);
        RedisCredentials c = p.get();
        assertEquals("alice", c.getUser());
        assertArrayEquals("s3cret".toCharArray(), c.getPassword());
    }

    @Test(expected = ConnectorException.class)
    public void basicWithoutPasswordThrowsDescriptive() {
        new BoomiRedisCredentialsProvider(AuthenticationType.BASIC, "alice", null, null).get();
    }

    @Test(expected = ConnectorException.class)
    public void entraWithoutContextThrows() {
        new BoomiRedisCredentialsProvider(
                AuthenticationType.MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL, null, null, null);
    }

    @Test
    public void entraExtractsOidAsUserAndTokenAsPassword() throws IOException {
        String token = jwtWithOid("11111111-2222-3333-4444-555555555555");
        OAuth2Context ctx = mock(OAuth2Context.class);
        OAuth2Token t = mock(OAuth2Token.class);
        when(t.getAccessToken()).thenReturn(token);
        when(ctx.getOAuth2Token(false)).thenReturn(t);

        BoomiRedisCredentialsProvider p = new BoomiRedisCredentialsProvider(
                AuthenticationType.MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL, null, null, ctx);
        RedisCredentials c = p.get();

        assertEquals("11111111-2222-3333-4444-555555555555", c.getUser());
        assertArrayEquals(token.toCharArray(), c.getPassword());
    }

    @Test(expected = ConnectorException.class)
    public void entraTokenFetchIOExceptionIsWrapped() throws IOException {
        OAuth2Context ctx = mock(OAuth2Context.class);
        when(ctx.getOAuth2Token(false)).thenThrow(new IOException("endpoint down"));
        new BoomiRedisCredentialsProvider(
                AuthenticationType.MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL, null, null, ctx).get();
    }

    @Test(expected = ConnectorException.class)
    public void entraMalformedTokenThrowsNotNpe() throws IOException {
        OAuth2Context ctx = mock(OAuth2Context.class);
        OAuth2Token t = mock(OAuth2Token.class);
        when(t.getAccessToken()).thenReturn("not-a-jwt");
        when(ctx.getOAuth2Token(false)).thenReturn(t);
        new BoomiRedisCredentialsProvider(
                AuthenticationType.MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL, null, null, ctx).get();
    }

    @Test
    public void entraTokenMissingOidThrowsNotNpe() throws IOException {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String noOid = enc.encodeToString("{\"alg\":\"none\"}".getBytes(StandardCharsets.UTF_8))
                + "." + enc.encodeToString("{\"sub\":\"x\"}".getBytes(StandardCharsets.UTF_8)) + ".";
        OAuth2Context ctx = mock(OAuth2Context.class);
        OAuth2Token t = mock(OAuth2Token.class);
        when(t.getAccessToken()).thenReturn(noOid);
        when(ctx.getOAuth2Token(false)).thenReturn(t);
        try {
            new BoomiRedisCredentialsProvider(
                    AuthenticationType.MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL, null, null, ctx).get();
            fail("expected ConnectorException");
        } catch (ConnectorException expected) {
            assertTrue(expected.getMessage().toLowerCase().contains("oid"));
        }
    }

    /** Mocks an OAuth2Context with a stable credential identity and a fixed token. */
    private static OAuth2Context entraContext(String clientId, String secret, String tokenUrl,
                                              String token) throws IOException {
        OAuth2Context ctx = mock(OAuth2Context.class);
        when(ctx.getClientId()).thenReturn(clientId);
        when(ctx.getClientSecret()).thenReturn(secret);
        when(ctx.getAccessTokenUrl()).thenReturn(tokenUrl);
        OAuth2Token t = mock(OAuth2Token.class);
        when(t.getAccessToken()).thenReturn(token);
        when(ctx.getOAuth2Token(false)).thenReturn(t);
        return ctx;
    }

    @Test
    public void entraStaleProviderFetchesThroughTheNewestContextForTheSameCredential() throws IOException {
        // The provider baked into a shared pooled client was built by execution 1. Later
        // executions reuse the client but construct their own provider with a LIVE OAuth2Context
        // for the same credential component. Token fetches must flow through the newest context:
        // a completed execution's context stops yielding fresh tokens, which surfaces on a real
        // Atom as WRONGPASS once the original token expires.
        OAuth2Context staleCtx = entraContext("client-1", "secret-1", "https://login/token",
                jwtWithOid("stale-execution-oid"));
        BoomiRedisCredentialsProvider baked = new BoomiRedisCredentialsProvider(
                AuthenticationType.MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL, null, null, staleCtx);

        OAuth2Context liveCtx = entraContext("client-1", "secret-1", "https://login/token",
                jwtWithOid("live-execution-oid"));
        new BoomiRedisCredentialsProvider(
                AuthenticationType.MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL, null, null, liveCtx);

        RedisCredentials c = baked.get();

        assertEquals("live-execution-oid", c.getUser());
        verify(liveCtx).getOAuth2Token(false);
        verify(staleCtx, never()).getOAuth2Token(false);
    }

    @Test
    public void entraContextForADifferentCredentialNeverHijacksAnotherProvider() throws IOException {
        // Registration is per credential identity (clientId + secret + token URL): a provider for
        // credential A must keep using A's newest context even after some other credential B
        // registers a fresh context.
        OAuth2Context ctxA = entraContext("client-A", "secret-A", "https://login/token",
                jwtWithOid("oid-A"));
        BoomiRedisCredentialsProvider pA = new BoomiRedisCredentialsProvider(
                AuthenticationType.MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL, null, null, ctxA);

        OAuth2Context ctxB = entraContext("client-B", "secret-B", "https://login/token",
                jwtWithOid("oid-B"));
        new BoomiRedisCredentialsProvider(
                AuthenticationType.MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL, null, null, ctxB);

        assertEquals("oid-A", pA.get().getUser());
        verify(ctxB, never()).getOAuth2Token(false);
    }

    @Test
    public void concurrentGetSerializesTokenFetch() throws Exception {
        final String token = jwtWithOid("aaaa");
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger maxObserved = new AtomicInteger();
        OAuth2Context ctx = mock(OAuth2Context.class);
        OAuth2Token t = mock(OAuth2Token.class);
        when(t.getAccessToken()).thenReturn(token);
        when(ctx.getOAuth2Token(false)).thenAnswer(inv -> {
            int now = inFlight.incrementAndGet();
            maxObserved.accumulateAndGet(now, Math::max);
            Thread.sleep(20);
            inFlight.decrementAndGet();
            return t;
        });
        final BoomiRedisCredentialsProvider p = new BoomiRedisCredentialsProvider(
                AuthenticationType.MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL, null, null, ctx);

        Thread[] threads = new Thread[8];
        for (int i = 0; i < threads.length; i++) threads[i] = new Thread(p::get);
        for (Thread th : threads) th.start();
        for (Thread th : threads) th.join();

        assertEquals("token fetch must be single-flight", 1, maxObserved.get());
    }
}
