package com.boomi.connector.redis;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.SimpleBrowseContext;

import static com.boomi.connector.api.ObjectDefinitionRole.INPUT;
import static com.boomi.connector.api.ObjectDefinitionRole.OUTPUT;
import static org.junit.Assert.assertNotNull;

@Category(IntegrationTest.class)
public class RedisBrowserTest {

    private static final Logger LOGGER = Logger.getLogger(RedisBrowserTest.class.getName());
    private Properties testConfig;

    private void loadTestProperties() throws IOException {
        testConfig = new Properties();
        try {
            testConfig.load(getClass().getResourceAsStream("/msEntraAuth.properties"));
        } catch (IOException e) {
            LOGGER.severe("Failed to load msEntraAuth.properties from src/test/resources");
            throw e;
        }
    }

    @Test
    public void testGetObjectDefinitions_GET() throws IOException {
        loadTestProperties();

        Map<String, Object> connProps = new HashMap<>();
        connProps.put("hosts", testConfig.getProperty("redis.host"));
        connProps.put("useSSL", Boolean.parseBoolean(testConfig.getProperty("redis.ssl")));
        connProps.put("authenticationType", "MicrosoftEntraClientSecretCredential");
        connProps.put("poolEnabled", Boolean.parseBoolean(testConfig.getProperty("redis.pool.enabled")));

        if (testConfig.getProperty("redis.connection.timeout") != null) {
            connProps.put("connectionTimeout", Long.parseLong(testConfig.getProperty("redis.connection.timeout")));
        }
        if (testConfig.getProperty("redis.socket.timeout") != null) {
            connProps.put("socketTimeout", Long.parseLong(testConfig.getProperty("redis.socket.timeout")));
        }
        if (testConfig.getProperty("redis.pool.size") != null && !testConfig.getProperty("redis.pool.size").isEmpty()) {
            connProps.put("poolSize", Long.parseLong(testConfig.getProperty("redis.pool.size")));
        }

        Map<String, Object> opProps = new HashMap<>();
        opProps.put("key_prefix", testConfig.getProperty("redis.cache.name"));
        opProps.put("remove_key_prefix_from_response", true);
        opProps.put("throw_exception", Boolean.parseBoolean(testConfig.getProperty("redis.throw.exception")));

        SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.GET, connProps, opProps);
        RedisConnection conn = new RedisConnection(context);
        RedisBrowser redisBrowser = new RedisBrowser(conn);

        ObjectDefinitions definitions = redisBrowser.getObjectDefinitions("Get", Arrays.asList(INPUT, OUTPUT));
        assertNotNull("Object definitions should not be null", definitions);
        LOGGER.info("testGetObjectDefinitions_GET passed with " + definitions.getDefinitions().size() + " definition(s)");
    }
}
