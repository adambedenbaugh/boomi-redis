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
public class CacheConnectorBasicAuthGetTest {

    private static final Logger LOGGER = Logger.getLogger(CacheConnectorBasicAuthGetTest.class.getName());
    private Properties testConfig;

    private void loadTestProperties() throws IOException {
        testConfig = new Properties();
        try {
            testConfig.load(getClass().getResourceAsStream("/basicAuth.properties"));
        } catch (IOException e) {
            LOGGER.severe("Failed to load test properties. Make sure basicAuth.properties exists in src/test/resources");
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
        connProps.put("password", testConfig.getProperty("redis.password"));
        connProps.put("user", testConfig.getProperty("redis.user"));
        connProps.put("authenticationType", "Basic");
        connProps.put("poolEnabled", Boolean.parseBoolean(testConfig.getProperty("redis.pool.enabled")));
        connProps.put("poolSize", Long.parseLong(testConfig.getProperty("redis.pool.size")));

        // Operation properties
        Map<String, Object> opProps = new HashMap<>();
        opProps.put("key_prefix", testConfig.getProperty("redis.cache.name"));
        opProps.put("remove_key_prefix_from_response", true);
        opProps.put("throw_exception", Boolean.parseBoolean(testConfig.getProperty("redis.throw.exception")));
        opProps.put("set_ttl", Long.parseLong(testConfig.getProperty("redis.set.ttl")));

        tester.setOperationContext(OperationType.GET, connProps, opProps, "Get", null);

        List<SimpleOperationResult> actualResults = tester.executeGetOperation("boomi");
        LOGGER.info("Actual Results: " + actualResults);
        assertNotNull("Results should not be null", actualResults);
        assertFalse("Should have at least one result", actualResults.isEmpty());
    }
}
