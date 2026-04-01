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

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RedisUpsertOperationTest {

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
        opProps.put("set_ttl", -1L);

        tester.setOperationContext(OperationType.UPSERT, connProps, opProps, "Upsert", null);
    }

    @Test
    public void testUpsertCallsSetWithCombinedKey() throws Exception {
        String payload = "{\"key\": \"mykey\", \"value\": \"myvalue\"}";

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class)) {

            List<SimpleOperationResult> results = tester.executeUpsertOperation(
                    Collections.singletonList(new ByteArrayInputStream(payload.getBytes())));

            assertFalse("Should have at least one result", results.isEmpty());
            assertEquals(OperationStatus.SUCCESS, results.get(0).getStatus());
            verify(mocked.constructed().get(0)).set("prefix:mykey", "myvalue", -1L);
        }
    }

    @Test
    public void testUpsertWithTtlPassesTtlToSet() throws Exception {
        opProps.put("set_ttl", 60000L);
        tester.setOperationContext(OperationType.UPSERT, connProps, opProps, "Upsert", null);
        String payload = "{\"key\": \"k\", \"value\": \"v\"}";

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class)) {

            List<SimpleOperationResult> results = tester.executeUpsertOperation(
                    Collections.singletonList(new ByteArrayInputStream(payload.getBytes())));

            assertFalse(results.isEmpty());
            assertEquals(OperationStatus.SUCCESS, results.get(0).getStatus());
            verify(mocked.constructed().get(0)).set("prefix:k", "v", 60000L);
        }
    }

    @Test
    public void testUpsertWithMalformedJsonAddsFailure() throws Exception {
        String payload = "not-valid-json";

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class)) {

            List<SimpleOperationResult> results = tester.executeUpsertOperation(
                    Collections.singletonList(new ByteArrayInputStream(payload.getBytes())));

            assertFalse(results.isEmpty());
            assertEquals(OperationStatus.FAILURE, results.get(0).getStatus());
        }
    }

    @Test
    public void testUpsertWithNullValuePassesNullToSet() throws Exception {
        String payload = "{\"key\": \"mykey\", \"value\": null}";

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class)) {

            List<SimpleOperationResult> results = tester.executeUpsertOperation(
                    Collections.singletonList(new ByteArrayInputStream(payload.getBytes())));

            assertFalse(results.isEmpty());
            assertEquals(OperationStatus.SUCCESS, results.get(0).getStatus());
            verify(mocked.constructed().get(0)).set("prefix:mykey", null, -1L);
        }
    }
}
