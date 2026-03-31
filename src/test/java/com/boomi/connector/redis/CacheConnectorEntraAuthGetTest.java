package com.boomi.connector.redis;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.redis.authentication.MicrosoftEntraClientSecretCredential;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.SimpleOperationResult;

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
        // Configure logging to show all levels
        // Logger rootLogger = Logger.getLogger("");
        // rootLogger.setLevel(Level.ALL);
        // ConsoleHandler handler = new ConsoleHandler();
        // handler.setLevel(Level.ALL);
        // rootLogger.addHandler(handler);

        // Load test configuration
        loadTestProperties();

        RedisConnector connector = new RedisConnector();
        ConnectorTester tester = new ConnectorTester(connector);
        
        // Connection properties
        Map<String, Object> connProps = new HashMap<>();
        connProps.put("hosts", testConfig.getProperty("redis.host"));
        connProps.put("useSSL", Boolean.parseBoolean(testConfig.getProperty("redis.ssl")));
        connProps.put("clientId", testConfig.getProperty("azure.client.id"));
        connProps.put("clientSecret", testConfig.getProperty("azure.client.secret"));
        connProps.put("tenantId", testConfig.getProperty("azure.tenant.id"));
        connProps.put("authenticationType", "MicrosoftEntraClientSecretCredential");
        connProps.put("poolEnabled", true);
        System.out.println("Pool Enabled: " + connProps.get("poolEnabled"));
        // Add timeout configurations
        if(testConfig.getProperty("redis.connection.timeout") != null) {
            connProps.put("connectionTimeout", Long.parseLong(testConfig.getProperty("redis.connection.timeout")));
        }
        if(testConfig.getProperty("redis.socket.timeout") != null) {
            connProps.put("socketTimeout", Long.parseLong(testConfig.getProperty("redis.socket.timeout")));
        }
        
        if(testConfig.getProperty("redis.pool.size") != null && !testConfig.getProperty("redis.pool.size").isEmpty()) {
            System.out.println("Using pool size: " + testConfig.getProperty("redis.pool.size"));
            connProps.put("poolSize", Long.parseLong(testConfig.getProperty("redis.pool.size")));
        } 
        

        // Operation properties
        Map<String, Object> opProps = new HashMap<>();
        opProps.put("key_prefix", testConfig.getProperty("redis.cache.name"));
        opProps.put("remove_key_prefix_from_response", Boolean.parseBoolean(testConfig.getProperty("redis.cache.remove.prefix")));
        opProps.put("wrap_inprofile", Boolean.parseBoolean(testConfig.getProperty("redis.wrap.inprofile")));
        opProps.put("throw_exception", Boolean.parseBoolean(testConfig.getProperty("redis.throw.exception")));
        opProps.put("set_ttl", Long.parseLong("60000")); 
        opProps.put("hashing", Boolean.parseBoolean(testConfig.getProperty("redis.hashing")));

        // Set up the operation context once for both operations
        tester.setOperationContext(OperationType.UPSERT, connProps, opProps, "Upsert", null);
        
        // First, perform an UPSERT operation to put data into Redis
        String upsertPayload = "{\"key\": \"12344\", \"value\": \"Test Value from Upsert Operation\"}";
        java.util.List<java.io.InputStream> upsertInputs = new java.util.ArrayList<>();
        upsertInputs.add(new java.io.ByteArrayInputStream(upsertPayload.getBytes()));
        List<SimpleOperationResult> upsertResults = tester.executeUpsertOperation(upsertInputs);
        System.out.println("Upsert Results: " + upsertResults);
        //Thread.sleep(60 * 60 * 1000); // Sleep for 1 hour to test token validity
        
        // Switch to GET operation context using the same connector instance
        tester.setOperationContext(OperationType.GET, connProps, opProps, "Get", null);

        List<SimpleOperationResult> actualResults = tester.executeGetOperation("12344");
        System.out.println("Get Results: " + actualResults);
    }

    @Test
    public void testMicrosoftEntraClientSecretCredential() throws IOException {
        System.out.println("Testing Microsoft Entra Client Secret Credential Start...");
        loadTestProperties();
        System.out.println("Testing Microsoft Entra Client Secret Credential...");
        new MicrosoftEntraClientSecretCredential(
            testConfig.getProperty("azure.tenant.id"),
            testConfig.getProperty("azure.client.id"),
            testConfig.getProperty("azure.client.secret")
        );

    }
}
