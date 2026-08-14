package com.boomi.connector.redis;

import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.OAuth2Token;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.redis.pool.RedisClientPoolManager;
import com.boomi.connector.redis.testutil.FakeJwt;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.SimpleOperationResult;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end UPSERT -> GET through the SDK's {@link ConnectorTester} operation harness using
 * Microsoft Entra authentication against a throwaway local Redis. Uses the same harness as
 * {@link RedisEntraPoolingIT}: an ACL user named after the token's {@code oid} claim accepts
 * {@code AUTH <oid> <any-token>}, and the {@link OAuth2Context} is a Mockito mock injected
 * straight into the connection property map — which is exactly how the SDK test-util hands it
 * to the connector. Fully self-contained: no Azure, no properties files, no credentials on disk.
 */
@Category(IntegrationTest.class)
public class RedisEntraOperationIT {

    private static final String OID = "11111111-2222-3333-4444-555555555555";
    private static final String ADMIN_PASS = "adminpass";
    private static final String KEY_PREFIX = "it:";

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
        // Entra-style nopass user whose name is the oid GUID (see CLAUDE.md: Testcontainers Entra harness).
        org.testcontainers.containers.Container.ExecResult r = REDIS.execInContainer(
                "redis-cli", "-a", ADMIN_PASS, "ACL", "SETUSER", OID, "on", "nopass", "~*", "+@all");
        assertEquals("ACL SETUSER failed: " + r.getStderr(), 0, r.getExitCode());
    }

    @AfterClass
    public static void stopRedis() {
        REDIS.stop();
    }

    @Before
    public void resetSharedPoolsBefore() {
        RedisClientPoolManager.closeAll();
    }

    @After
    public void resetSharedPoolsAfter() {
        RedisClientPoolManager.closeAll();
    }

    private static OAuth2Context entraContext() throws Exception {
        OAuth2Context ctx = mock(OAuth2Context.class);
        when(ctx.getClientId()).thenReturn("test-client");
        OAuth2Token token = mock(OAuth2Token.class);
        when(token.getAccessToken()).thenReturn(FakeJwt.token(OID));
        when(ctx.getOAuth2Token(false)).thenReturn(token);
        return ctx;
    }

    private static Map<String, Object> entraConnProps(OAuth2Context ctx) {
        Map<String, Object> connProps = new HashMap<>();
        connProps.put("hosts", hostPort);
        connProps.put("useSSL", false);
        connProps.put("authenticationType", "MicrosoftEntraClientSecretCredential");
        connProps.put("entraOAuth2", ctx);
        connProps.put("poolEnabled", true);
        connProps.put("poolSize", 2L);
        connProps.put("minPoolSize", 1L);
        connProps.put("connectionTimeout", 5L);
        connProps.put("socketTimeout", 5L);
        return connProps;
    }

    private static Map<String, Object> opProps() {
        Map<String, Object> opProps = new HashMap<>();
        opProps.put("key_prefix", KEY_PREFIX);
        opProps.put("remove_key_prefix_from_response", true);
        opProps.put("throw_exception", true);
        return opProps;
    }

    @Test
    public void entraUpsertThenGetRoundTripsThroughTheOperationLayer() throws Exception {
        RedisConnector connector = new RedisConnector();
        ConnectorTester tester = new ConnectorTester(connector);
        OAuth2Context ctx = entraContext();

        // UPSERT
        tester.setOperationContext(OperationType.UPSERT, entraConnProps(ctx), opProps(), "Upsert", null);
        String upsertPayload = "{\"key\": \"entra-key\", \"value\": \"Entra auth round trip value\"}";
        List<InputStream> upsertInputs = new ArrayList<>();
        upsertInputs.add(new ByteArrayInputStream(upsertPayload.getBytes(StandardCharsets.UTF_8)));
        List<SimpleOperationResult> upsertResults = tester.executeUpsertOperation(upsertInputs);

        assertEquals("expected exactly one upsert result", 1, upsertResults.size());
        assertEquals("upsert must succeed; message: " + upsertResults.get(0).getMessage(),
                OperationStatus.SUCCESS, upsertResults.get(0).getStatus());

        // GET
        tester.setOperationContext(OperationType.GET, entraConnProps(ctx), opProps(), "Get", null);
        List<SimpleOperationResult> getResults = tester.executeGetOperation("entra-key");

        assertEquals("expected exactly one get result", 1, getResults.size());
        SimpleOperationResult getResult = getResults.get(0);
        assertEquals("get must succeed; message: " + getResult.getMessage(),
                OperationStatus.SUCCESS, getResult.getStatus());
        assertFalse("get must return a payload", getResult.getPayloads().isEmpty());

        String json = new String(getResult.getPayloads().get(0), StandardCharsets.UTF_8);
        assertTrue("payload must contain the upserted value; got: " + json,
                json.contains("Entra auth round trip value"));
        assertTrue("prefix must be stripped from the returned key; got: " + json,
                json.contains("\"key\": \"entra-key\""));
    }
}
