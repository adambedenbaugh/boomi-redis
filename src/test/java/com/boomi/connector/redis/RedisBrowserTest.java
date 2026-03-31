package com.boomi.connector.redis;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.SimpleBrowseContext;

import static com.boomi.connector.api.ObjectDefinitionRole.INPUT;
import static com.boomi.connector.api.ObjectDefinitionRole.OUTPUT;

public class RedisBrowserTest {
    private Properties testConfig;
    private void loadTestProperties() throws IOException {
        testConfig = new Properties();
        try {
            testConfig.load(getClass().getResourceAsStream("/msEntraAuth.properties"));
        } catch (IOException e) {
            throw e;
        }
    }

	@Test
	public void testgetObjectDefinitions_GET() throws IOException {
        // Load test configuration
        loadTestProperties();
        // Connection properties
        Map<String, Object> connProps = new HashMap<>();
        connProps.put("hosts", testConfig.getProperty("redis.host"));
        connProps.put("useSSL", Boolean.parseBoolean(testConfig.getProperty("redis.ssl")));
        connProps.put("clientId", testConfig.getProperty("azure.client.id"));
        connProps.put("clientSecret", testConfig.getProperty("azure.client.secret"));
        connProps.put("tenantId", testConfig.getProperty("azure.tenant.id"));
        connProps.put("authenticationType", "MicrosoftEntraClientSecretCredential");
        connProps.put("poolEnabled", Boolean.parseBoolean(testConfig.getProperty("redis.pool.enabled")));
        System.out.println("Pool enabled: " + testConfig.getProperty("redis.pool.enabled"));
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
        System.out.println("Does this work3");

        // Operation properties
        Map<String, Object> opProps = new HashMap<>();
        opProps.put("key_prefix", testConfig.getProperty("redis.cache.name"));
        opProps.put("remove_key_prefix_from_response", Boolean.parseBoolean(testConfig.getProperty("redis.cache.remove.prefix")));
        opProps.put("auto_key", Boolean.parseBoolean(testConfig.getProperty("redis.auto.key")));
        opProps.put("wrap_inprofile", Boolean.parseBoolean(testConfig.getProperty("redis.wrap.inprofile")));
        opProps.put("throw_exception", Boolean.parseBoolean(testConfig.getProperty("redis.throw.exception")));
        opProps.put("set_ttl", Long.parseLong(testConfig.getProperty("redis.set.ttl"))); 
        opProps.put("hashing", Boolean.parseBoolean(testConfig.getProperty("redis.hashing")));



        
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.GET, connProps, opProps);
		RedisConnection conn = new RedisConnection(context);
		RedisBrowser redisBrowser = new RedisBrowser(conn);

		redisBrowser.getObjectDefinitions("Get", Arrays.asList(INPUT, OUTPUT));
        System.out.println("testgetObjectDefinitions_GET passed");
	}

}
