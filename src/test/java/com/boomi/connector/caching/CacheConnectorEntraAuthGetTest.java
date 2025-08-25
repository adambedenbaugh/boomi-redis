package com.boomi.connector.caching;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.ehcache.Cache;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.caching.authentication.MicrosoftEntraClientSecretCredential;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.SimpleOperationResult;

public class CacheConnectorEntraAuthGetTest {
    // static {
    //     System.setProperty("io.netty.handler.ssl.noOpenSsl", "true");
    //     System.setProperty("io.netty.noNative", "true");
    //     System.setProperty("io.netty.noNativeEventLoop", "true");
    //     System.setProperty("reactor.netty.http.native.useNativeTransport", "false");
        
    //     // Suppress Netty logging
    //     java.util.logging.Logger.getLogger("io.netty").setLevel(java.util.logging.Level.WARNING);
    // }
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

    public void testGetOperation() throws Exception {
        // Configure logging to show all levels
        Logger rootLogger = Logger.getLogger("");
        rootLogger.setLevel(Level.ALL);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.ALL);
        rootLogger.addHandler(handler);

        // Load test configuration
        loadTestProperties();

        CacheConnector connector = new CacheConnector();
        ConnectorTester tester = new ConnectorTester(connector);
        
        // Connection properties
        Map<String, Object> connProps = new HashMap<>();
        connProps.put("type", "com.boomi.proserv.caching.impl.CacheRedis");
        connProps.put("heap", "1024");
        connProps.put("hosts", testConfig.getProperty("redis.host"));
        connProps.put("useSSL", Boolean.parseBoolean(testConfig.getProperty("redis.ssl")));
        connProps.put("clientId", testConfig.getProperty("azure.client.id"));
        connProps.put("clientSecret", testConfig.getProperty("azure.client.secret"));
        connProps.put("tenantId", testConfig.getProperty("azure.tenant.id"));
        connProps.put("authenticationType", "MicrosoftEntraClientSecretCredential");
        connProps.put("poolEnabled", Boolean.parseBoolean(testConfig.getProperty("redis.pool.enabled")));
        if(testConfig.getProperty("redis.pool.size") != null && !testConfig.getProperty("redis.pool.size").isEmpty()) {
            System.out.println("Using pool size: " + testConfig.getProperty("redis.pool.size"));
            connProps.put("poolSize", Long.parseLong(testConfig.getProperty("redis.pool.size")));
        } 
        

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


    public static void main(String[] args) {
		try {
            //new CacheConnectorEntraAuthGetTest().testMicrosoftEntraClientSecretCredential();
			new CacheConnectorEntraAuthGetTest().testGetOperation();
            System.out.println("Test completed successfully.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
