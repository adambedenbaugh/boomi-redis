package com.boomi.connector.redis;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.SimpleOperationResult;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@Category(IntegrationTest.class)
public class CacheConnectorEntraAuthGetTest {

    private static final Logger LOGGER = Logger.getLogger(CacheConnectorEntraAuthGetTest.class.getName());
    private Properties testConfig;

    private void loadTestProperties() throws IOException {
        testConfig = new Properties();
        try {
            testConfig.load(getClass().getResourceAsStream("/msEntraAuth.properties"));
        } catch (IOException e) {
            LOGGER.severe("Failed to load test properties. Make sure msEntraAuth.properties exists in src/test/resources");
            throw e;
        }
    }

    @Test
    public void testGetOperation() throws Exception {
        loadTestProperties();

        RedisConnector connector = new RedisConnector();
        ConnectorTester tester = new ConnectorTester(connector);

        // Connection properties
        Map<String, Object> connProps = new HashMap<>();
        connProps.put("hosts", testConfig.getProperty("redis.host"));
        connProps.put("useSSL", Boolean.parseBoolean(testConfig.getProperty("redis.ssl")));
        connProps.put("authenticationType", "MicrosoftEntraClientSecretCredential");
        connProps.put("poolEnabled", true);

        if (testConfig.getProperty("redis.connection.timeout") != null) {
            connProps.put("connectionTimeout", Long.parseLong(testConfig.getProperty("redis.connection.timeout")));
        }
        if (testConfig.getProperty("redis.socket.timeout") != null) {
            connProps.put("socketTimeout", Long.parseLong(testConfig.getProperty("redis.socket.timeout")));
        }
        if (testConfig.getProperty("redis.pool.size") != null && !testConfig.getProperty("redis.pool.size").isEmpty()) {
            connProps.put("poolSize", Long.parseLong(testConfig.getProperty("redis.pool.size")));
        }

        // Operation properties
        Map<String, Object> opProps = new HashMap<>();
        opProps.put("key_prefix", testConfig.getProperty("redis.cache.name"));
        opProps.put("remove_key_prefix_from_response", Boolean.parseBoolean(testConfig.getProperty("redis.cache.remove.prefix")));
        opProps.put("throw_exception", Boolean.parseBoolean(testConfig.getProperty("redis.throw.exception")));
        opProps.put("set_ttl", 60000L);

        // First, upsert test data into Redis
        tester.setOperationContext(OperationType.UPSERT, connProps, opProps, "Upsert", null);
        String upsertPayload = "{\"key\": \"12344\", \"value\": \"Test Value from Upsert Operation\"}";
        List<java.io.InputStream> upsertInputs = new java.util.ArrayList<>();
        upsertInputs.add(new java.io.ByteArrayInputStream(upsertPayload.getBytes()));
        List<SimpleOperationResult> upsertResults = tester.executeUpsertOperation(upsertInputs);
        LOGGER.info("Upsert Results: " + upsertResults);
        assertNotNull("Upsert results should not be null", upsertResults);
        assertFalse("Upsert should have at least one result", upsertResults.isEmpty());

        // Then retrieve it
        tester.setOperationContext(OperationType.GET, connProps, opProps, "Get", null);
        List<SimpleOperationResult> getResults = tester.executeGetOperation("12344");
        LOGGER.info("Get Results: " + getResults);
        assertNotNull("Get results should not be null", getResults);
        assertFalse("Get should have at least one result", getResults.isEmpty());
    }
}
