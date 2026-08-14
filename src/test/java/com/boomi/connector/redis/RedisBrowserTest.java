package com.boomi.connector.redis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.junit.Test;

import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.SimpleBrowseContext;

import static com.boomi.connector.api.ObjectDefinitionRole.INPUT;
import static com.boomi.connector.api.ObjectDefinitionRole.OUTPUT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Browsing only serves the bundled JSON schemas — it never opens a Redis connection — so this
 * runs as a plain unit test with placeholder connection settings: no Docker, no properties files.
 */
public class RedisBrowserTest {

    private static final Logger LOGGER = Logger.getLogger(RedisBrowserTest.class.getName());

    @Test
    public void testGetObjectDefinitions_GET() {
        Map<String, Object> connProps = new HashMap<>();
        connProps.put("hosts", "localhost:6379");
        connProps.put("useSSL", false);
        connProps.put("authenticationType", "None");
        connProps.put("poolEnabled", false);

        Map<String, Object> opProps = new HashMap<>();
        opProps.put("key_prefix", "");
        opProps.put("remove_key_prefix_from_response", true);
        opProps.put("throw_exception", false);

        SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.GET, connProps, opProps);
        RedisConnection conn = new RedisConnection(context);
        RedisBrowser redisBrowser = new RedisBrowser(conn);

        ObjectDefinitions definitions = redisBrowser.getObjectDefinitions("Get", Arrays.asList(INPUT, OUTPUT));
        assertNotNull("Object definitions should not be null", definitions);
        assertFalse("Expected at least one object definition", definitions.getDefinitions().isEmpty());
        LOGGER.info("testGetObjectDefinitions_GET passed with " + definitions.getDefinitions().size() + " definition(s)");
    }
}
