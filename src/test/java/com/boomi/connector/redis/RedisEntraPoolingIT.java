package com.boomi.connector.redis;

import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.OAuth2Token;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.redis.testutil.FakeJwt;
import com.boomi.connector.testutil.SimpleBrowseContext;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@Category(IntegrationTest.class)
public class RedisEntraPoolingIT {

    private static final String OID = "11111111-2222-3333-4444-555555555555";
    private static final String ADMIN_PASS = "adminpass";

    @SuppressWarnings("resource")
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7"))
                    .withExposedPorts(6379)
                    .withCommand("redis-server", "--requirepass", ADMIN_PASS);

    private static String hostPort;

    @BeforeClass
    public static void startRedis() throws Exception {
        REDIS.start();
        hostPort = REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
        // Entra-style nopass user whose name is the oid GUID.
        exec("redis-cli", "-a", ADMIN_PASS, "ACL", "SETUSER", OID, "on", "nopass", "~*", "+@all");
    }

    @AfterClass
    public static void stopRedis() {
        REDIS.stop();
    }

    @Before
    public void resetSharedPoolsBefore() {
        com.boomi.connector.redis.connection.StandalonePooledRedisConnection.closeAllSharedPools();
    }

    @After
    public void resetSharedPoolsAfter() {
        com.boomi.connector.redis.connection.StandalonePooledRedisConnection.closeAllSharedPools();
    }

    private static void exec(String... cmd) throws Exception {
        org.testcontainers.containers.Container.ExecResult r = REDIS.execInContainer(cmd);
        assertEquals("cmd failed: " + String.join(" ", cmd) + " -> " + r.getStderr(), 0, r.getExitCode());
    }

    private static OAuth2Context entraContextReturning(String... tokensInOrder) throws Exception {
        OAuth2Context ctx = mock(OAuth2Context.class);
        when(ctx.getClientId()).thenReturn("test-client");
        OAuth2Token[] toks = new OAuth2Token[tokensInOrder.length];
        for (int i = 0; i < tokensInOrder.length; i++) {
            OAuth2Token t = mock(OAuth2Token.class);
            when(t.getAccessToken()).thenReturn(tokensInOrder[i]);
            toks[i] = t;
        }
        if (toks.length == 1) {
            when(ctx.getOAuth2Token(false)).thenReturn(toks[0]);
        } else {
            OAuth2Token[] rest = new OAuth2Token[toks.length - 1];
            System.arraycopy(toks, 1, rest, 0, rest.length);
            when(ctx.getOAuth2Token(false)).thenReturn(toks[0], rest);
        }
        return ctx;
    }

    private static RedisConnection pooledEntraConnection(OAuth2Context ctx) {
        Map<String, Object> conn = new HashMap<>();
        conn.put("hosts", hostPort);
        conn.put("useSSL", false);
        conn.put("authenticationType", "MicrosoftEntraClientSecretCredential");
        conn.put("entraOAuth2", ctx);
        conn.put("poolEnabled", true);
        conn.put("poolSize", 2L);
        conn.put("minPoolSize", 1L);
        conn.put("connectionTimeout", 5L);
        conn.put("socketTimeout", 5L);
        SimpleBrowseContext bc = new SimpleBrowseContext(null, null, OperationType.GET, conn, new HashMap<>());
        return new RedisConnection(bc);
    }

    @Test
    public void entraPooledSetGetDeleteAuthenticatesEndToEnd() throws Exception {
        OAuth2Context ctx = entraContextReturning(FakeJwt.token(OID));
        RedisConnection conn = pooledEntraConnection(ctx);
        try {
            conn.set("greeting", "hello", -1L);
            assertEquals("hello", conn.get("greeting"));
            conn.del("greeting");
            assertNull(conn.get("greeting"));
        } finally {
            conn.close();
        }
    }

    @Test
    public void tokenRotationSelfHealsAfterConnectionKill() throws Exception {
        // First token used initially; after the pooled connections are killed, the pool rebuilds
        // and the provider hands over the second token.
        OAuth2Context ctx = entraContextReturning(FakeJwt.token(OID), FakeJwt.token(OID));
        RedisConnection conn = pooledEntraConnection(ctx);
        try {
            conn.set("k", "v1", -1L);
            assertEquals("v1", conn.get("k"));

            // Force all normal client connections closed; next borrow must rebuild + re-auth.
            exec("redis-cli", "-a", ADMIN_PASS, "CLIENT", "KILL", "TYPE", "normal");

            assertEquals("v1", conn.get("k"));                 // succeeds despite the kill
            verify(ctx, atLeast(2)).getOAuth2Token(false);     // provider was asked again
        } finally {
            conn.close();
        }
    }

    @Test
    public void missingOidClaimFailsWithDescriptiveError() throws Exception {
        OAuth2Context ctx = mock(OAuth2Context.class);
        when(ctx.getClientId()).thenReturn("test-client");
        OAuth2Token t = mock(OAuth2Token.class);
        java.util.Base64.Encoder enc = java.util.Base64.getUrlEncoder().withoutPadding();
        String noOid = enc.encodeToString("{\"alg\":\"none\"}".getBytes("UTF-8"))
                + "." + enc.encodeToString("{\"sub\":\"x\"}".getBytes("UTF-8")) + ".";
        when(t.getAccessToken()).thenReturn(noOid);
        when(ctx.getOAuth2Token(false)).thenReturn(t);
        RedisConnection conn = pooledEntraConnection(ctx);
        try {
            conn.get("anything");
            fail("expected a descriptive failure about the missing oid claim");
        } catch (RuntimeException e) {
            boolean mentionsOid = false;
            for (Throwable cur = e; cur != null; cur = cur.getCause()) {
                if (cur.getMessage() != null && cur.getMessage().toLowerCase().contains("oid")) {
                    mentionsOid = true;
                    break;
                }
            }
            assertTrue("error chain must name the missing 'oid' claim so the user can fix it; got: " + e, mentionsOid);
        } finally {
            conn.close();
        }
    }
}
