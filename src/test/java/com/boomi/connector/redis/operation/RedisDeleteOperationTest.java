package com.boomi.connector.redis.operation;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.redis.RedisConnector;
import com.boomi.connector.redis.RedisConnection;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.SimpleOperationResult;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RedisDeleteOperationTest {

    private ConnectorTester tester;
    private Map<String, Object> connProps;
    private Map<String, Object> opProps;

    @Before
    public void setUp() {
        tester = new ConnectorTester(new RedisConnector());

        connProps = new HashMap<>();
        connProps.put("hosts", "localhost:6379");
        connProps.put("useSSL", false);
        connProps.put("authenticationType", "None");
        connProps.put("poolEnabled", false);

        opProps = new HashMap<>();
        opProps.put("key_prefix", "prefix:");

        tester.setOperationContext(OperationType.DELETE, connProps, opProps, "Delete", null);
    }

    @Test
    public void testSingleDeleteCallsDelWithCombinedKey() throws Exception {
        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class)) {

            List<SimpleOperationResult> results = tester.executeDeleteOperation(Collections.singletonList("mykey"));

            assertFalse("Should have at least one result", results.isEmpty());
            assertEquals(OperationStatus.SUCCESS, results.get(0).getStatus());
            verify(mocked.constructed().get(0)).del("prefix:mykey");
        }
    }

    @Test
    public void testWildcardDeleteCallsDelAllWithRawPrefix() throws Exception {
        // The raw prefix is passed through: the connection layer's prepareScanPattern is the
        // single point that escapes glob metacharacters and appends the trailing wildcard.
        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class)) {

            List<SimpleOperationResult> results = tester.executeDeleteOperation(Collections.singletonList("*"));

            assertFalse(results.isEmpty());
            assertEquals(OperationStatus.SUCCESS, results.get(0).getStatus());
            verify(mocked.constructed().get(0)).delAll("prefix:");
        }
    }

    @Test
    public void testDeleteWithEmptyPrefixUsesKeyDirectly() throws Exception {
        opProps.put("key_prefix", "");
        tester.setOperationContext(OperationType.DELETE, connProps, opProps, "Delete", null);

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class)) {

            List<SimpleOperationResult> results = tester.executeDeleteOperation(Collections.singletonList("mykey"));

            assertFalse(results.isEmpty());
            verify(mocked.constructed().get(0)).del("mykey");
        }
    }

    @Test
    public void testDeleteAlwaysReturnsSuccessEvenIfKeyMissing() throws Exception {
        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class)) {

            List<SimpleOperationResult> results = tester.executeDeleteOperation(Collections.singletonList("nonexistent"));

            assertFalse(results.isEmpty());
            assertEquals(OperationStatus.SUCCESS, results.get(0).getStatus());
        }
    }

    @Test
    public void testMultipleObjectIdsDeletedInOneCall() throws Exception {
        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class)) {

            List<SimpleOperationResult> results = tester.executeDeleteOperation(Arrays.asList("key1", "key2"));

            assertEquals(2, results.size());
            verify(mocked.constructed().get(0)).del("prefix:key1");
            verify(mocked.constructed().get(0)).del("prefix:key2");
        }
    }

    @Test
    public void testWildcardDeletePassesPrefixWithMetacharactersUnescaped() throws Exception {
        // Escaping happens once, in the connection layer (prepareScanPattern) - see
        // StandaloneRedisConnectionTest.getAllEscapesGlobMetacharactersInPrefixBeforeScan.
        opProps.put("key_prefix", "cache[1]:");
        tester.setOperationContext(OperationType.DELETE, connProps, opProps, "Delete", null);

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class)) {

            tester.executeDeleteOperation(Collections.singletonList("*"));

            verify(mocked.constructed().get(0)).delAll("cache[1]:");
        }
    }

    @Test
    public void testConnectionIsClosedAfterExecute() throws Exception {
        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class)) {

            tester.executeDeleteOperation(Collections.singletonList("mykey"));

            verify(mocked.constructed().get(0)).close();
        }
    }
}
