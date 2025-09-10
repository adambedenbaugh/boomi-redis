package com.boomi.connector.redis;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.redis.RedisConnector;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.SimpleOperationResult;

public class CacheConnectorBasicAuthGetTest {
    private static final Logger LOGGER = Logger.getLogger(CacheConnectorBasicAuthGetTest.class.getName());
    private Properties testConfig;

    private void loadTestProperties() throws IOException {
        testConfig = new Properties();
        try {
            testConfig.load(getClass().getResourceAsStream("basicAuth.properties"));
        } catch (IOException e) {
            LOGGER.severe("Failed to load test properties. Make sure basicAuth.properties exists in src/test/resources");
            throw e;
        }
    }

    public void testGetOperation() throws Exception {
        // Configure logging to show all levels
        Logger rootLogger = Logger.getLogger("");
        rootLogger.setLevel(Level.ALL);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.ALL);
        rootLogger.addHandler(handler);

        // Load test configuration
        loadTestProperties();

        RedisConnector connector = new RedisConnector();
        ConnectorTester tester = new ConnectorTester(connector);
        
        // Connection properties
        Map<String, Object> connProps = new HashMap<>();
        connProps.put("type", "com.boomi.proserv.caching.impl.CacheRedis");
        connProps.put("heap", "1024");
        connProps.put("hosts", testConfig.getProperty("redis.host"));
        connProps.put("useSSL", Boolean.parseBoolean(testConfig.getProperty("redis.ssl")));
        connProps.put("password", testConfig.getProperty("redis.password"));
        connProps.put("user", testConfig.getProperty("redis.user"));
        connProps.put("authenticationType", "Basic");
        connProps.put("poolEnabled", Boolean.parseBoolean(testConfig.getProperty("redis.pool.enabled")));
        connProps.put("poolSize", Long.parseLong(testConfig.getProperty("redis.pool.size")));

        // Operation properties
        Map<String, Object> opProps = new HashMap<>();
        opProps.put("cache_name", testConfig.getProperty("redis.cache.name"));
        opProps.put("auto_key", Boolean.parseBoolean(testConfig.getProperty("redis.auto.key")));
        opProps.put("wrap_inprofile", Boolean.parseBoolean(testConfig.getProperty("redis.wrap.inprofile")));
        opProps.put("throw_exception", Boolean.parseBoolean(testConfig.getProperty("redis.throw.exception")));
        opProps.put("set_ttl", Integer.parseInt(testConfig.getProperty("redis.set.ttl"))); 
        opProps.put("hashing", Boolean.parseBoolean(testConfig.getProperty("redis.hashing")));

        tester.setOperationContext(OperationType.GET, connProps, opProps, "Get", null);

        List<SimpleOperationResult> actualResults = tester.executeGetOperation("boomi");
        LOGGER.info("Actual Results: " + actualResults);
    }

    public static void main(String[] args) {
		try {
			new CacheConnectorBasicAuthGetTest().testGetOperation();
            System.out.println("Test completed successfully.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
