package com.boomi.connector.redis;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.redis.pool.RedisClientPoolManager;
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

/**
 * End-to-end UPSERT -> GET through the SDK's {@link ConnectorTester} operation harness using
 * Basic (username/password) authentication against a throwaway local Redis. The ACL user is
 * created inside the container, so the test is fully self-contained: no external Redis, no
 * properties files, no credentials on disk.
 */
@Category(IntegrationTest.class)
public class RedisBasicAuthOperationIT {

    private static final String ADMIN_PASS = "adminpass";
    private static final String BASIC_USER = "boomi-basic";
    private static final String BASIC_PASS = "basic-secret";
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
        // Named ACL user so AUTH <user> <pass> is genuinely exercised (requirepass alone would
        // let a passwordless default-user connection through on some misconfigurations).
        org.testcontainers.containers.Container.ExecResult r = REDIS.execInContainer(
                "redis-cli", "-a", ADMIN_PASS, "ACL", "SETUSER", BASIC_USER,
                "on", ">" + BASIC_PASS, "~*", "+@all");
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

    private static Map<String, Object> basicAuthConnProps() {
        Map<String, Object> connProps = new HashMap<>();
        connProps.put("hosts", hostPort);
        connProps.put("useSSL", false);
        connProps.put("authenticationType", "Basic");
        connProps.put("user", BASIC_USER);
        connProps.put("password", BASIC_PASS);
        connProps.put("poolEnabled", false);
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
    public void basicAuthUpsertThenGetRoundTripsThroughTheOperationLayer() throws Exception {
        RedisConnector connector = new RedisConnector();
        ConnectorTester tester = new ConnectorTester(connector);

        // UPSERT
        tester.setOperationContext(OperationType.UPSERT, basicAuthConnProps(), opProps(), "Upsert", null);
        String upsertPayload = "{\"key\": \"basic-key\", \"value\": \"Basic auth round trip value\"}";
        List<InputStream> upsertInputs = new ArrayList<>();
        upsertInputs.add(new ByteArrayInputStream(upsertPayload.getBytes(StandardCharsets.UTF_8)));
        List<SimpleOperationResult> upsertResults = tester.executeUpsertOperation(upsertInputs);

        assertEquals("expected exactly one upsert result", 1, upsertResults.size());
        assertEquals("upsert must succeed; message: " + upsertResults.get(0).getMessage(),
                OperationStatus.SUCCESS, upsertResults.get(0).getStatus());

        // GET
        tester.setOperationContext(OperationType.GET, basicAuthConnProps(), opProps(), "Get", null);
        List<SimpleOperationResult> getResults = tester.executeGetOperation("basic-key");

        assertEquals("expected exactly one get result", 1, getResults.size());
        SimpleOperationResult getResult = getResults.get(0);
        assertEquals("get must succeed; message: " + getResult.getMessage(),
                OperationStatus.SUCCESS, getResult.getStatus());
        assertFalse("get must return a payload", getResult.getPayloads().isEmpty());

        String json = new String(getResult.getPayloads().get(0), StandardCharsets.UTF_8);
        assertTrue("payload must contain the upserted value; got: " + json,
                json.contains("Basic auth round trip value"));
        assertTrue("prefix must be stripped from the returned key; got: " + json,
                json.contains("\"key\": \"basic-key\""));
    }
}
