package com.boomi.connector.redis.operation;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.redis.IntegrationTest;
import com.boomi.connector.redis.RedisConnector;
import com.boomi.connector.redis.RedisConnection;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.MockedConstruction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RedisGetOperationTest {

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
        opProps.put("remove_key_prefix_from_response", true);
        opProps.put("throw_exception", false);

        tester.setOperationContext(OperationType.GET, connProps, opProps, "Get", null);
    }

    @Test
    public void testSingleGetCombinesKeyPrefix() throws Exception {
        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, ctx) -> when(mock.get("prefix:mykey")).thenReturn("my-value"))) {

            List<SimpleOperationResult> results = tester.executeGetOperation("mykey");

            assertFalse("Should have at least one result", results.isEmpty());
            assertEquals(OperationStatus.SUCCESS, results.get(0).getStatus());
            verify(mocked.constructed().get(0)).get("prefix:mykey");
        }
    }

    @Test
    public void testSingleGetRemovesPrefixFromResponseJson() throws Exception {
        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, ctx) -> when(mock.get("prefix:mykey")).thenReturn("my-value"))) {

            List<SimpleOperationResult> results = tester.executeGetOperation("mykey");

            assertFalse(results.isEmpty());
            String json = new String(results.get(0).getPayloads().get(0));
            JsonArray arr = JsonParser.parseString(json).getAsJsonArray();
            assertEquals("mykey", arr.get(0).getAsJsonObject().get("key").getAsString());
        }
    }

    @Test
    public void testSingleGetKeepsPrefixWhenFlagFalse() throws Exception {
        opProps.put("remove_key_prefix_from_response", false);
        tester.setOperationContext(OperationType.GET, connProps, opProps, "Get", null);

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, ctx) -> when(mock.get("prefix:mykey")).thenReturn("my-value"))) {

            List<SimpleOperationResult> results = tester.executeGetOperation("mykey");

            assertFalse(results.isEmpty());
            String json = new String(results.get(0).getPayloads().get(0));
            JsonArray arr = JsonParser.parseString(json).getAsJsonArray();
            assertEquals("prefix:mykey", arr.get(0).getAsJsonObject().get("key").getAsString());
        }
    }

    @Test
    public void testSingleGetNullValueReturnsEmptyResult() throws Exception {
        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, ctx) -> when(mock.get(anyString())).thenReturn(null))) {

            List<SimpleOperationResult> results = tester.executeGetOperation("missingkey");

            assertFalse(results.isEmpty());
            assertEquals(OperationStatus.SUCCESS, results.get(0).getStatus());
        }
    }

    @Test
    public void testThrowExceptionWhenKeyNotFound() throws Exception {
        opProps.put("throw_exception", true);
        tester.setOperationContext(OperationType.GET, connProps, opProps, "Get", null);

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, ctx) -> when(mock.get(anyString())).thenReturn(null))) {

            List<SimpleOperationResult> results = tester.executeGetOperation("missingkey");

            assertFalse(results.isEmpty());
            assertEquals(OperationStatus.FAILURE, results.get(0).getStatus());
        }
    }

    @Test
    public void testWildcardObjectIdCallsGetAll() throws Exception {
        Map<String, String> allKeys = new HashMap<>();
        allKeys.put("prefix:a", "val-a");
        allKeys.put("prefix:b", "val-b");

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, ctx) -> when(mock.getAll("prefix:")).thenReturn(allKeys))) {

            List<SimpleOperationResult> results = tester.executeGetOperation("*");

            assertFalse(results.isEmpty());
            assertEquals(OperationStatus.SUCCESS, results.get(0).getStatus());
            verify(mocked.constructed().get(0)).getAll("prefix:");
        }
    }

    @Test
    public void testWildcardWithNoMatchesReturnsEmptySuccess() throws Exception {
        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, ctx) -> when(mock.getAll("prefix:")).thenReturn(new HashMap<>()))) {

            List<SimpleOperationResult> results = tester.executeGetOperation("*");

            assertFalse(results.isEmpty());
            assertEquals(OperationStatus.SUCCESS, results.get(0).getStatus());
        }
    }

    @Test
    public void testThrowExceptionWhenWildcardHasNoMatches() throws Exception {
        opProps.put("throw_exception", true);
        tester.setOperationContext(OperationType.GET, connProps, opProps, "Get", null);

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, ctx) -> when(mock.getAll("prefix:")).thenReturn(new HashMap<>()))) {

            List<SimpleOperationResult> results = tester.executeGetOperation("*");

            assertFalse(results.isEmpty());
            assertEquals(OperationStatus.FAILURE, results.get(0).getStatus());
        }
    }

    @Test
    public void testConnectionIsClosedAfterExecute() throws Exception {
        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class,
                (mock, ctx) -> when(mock.get("prefix:mykey")).thenReturn("my-value"))) {

            tester.executeGetOperation("mykey");

            verify(mocked.constructed().get(0)).close();
        }
    }

    @Test
    public void testEmptyObjectIdFailsWithDescriptiveErrorWithoutTouchingRedis() throws Exception {
        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class)) {

            List<SimpleOperationResult> results = tester.executeGetOperation("");

            assertFalse(results.isEmpty());
            assertEquals(OperationStatus.FAILURE, results.get(0).getStatus());
            assertTrue("failure message must explain the empty id, got: " + results.get(0).getMessage(),
                    results.get(0).getMessage().toLowerCase().contains("empty id"));
            // Validation happens before init(), so a doomed request never opens a connection.
            verify(mocked.constructed().get(0), never()).init();
            verify(mocked.constructed().get(0), never()).get(anyString());
        }
    }
}
