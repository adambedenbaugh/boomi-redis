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

public class RedisSendOperationTest {

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
        opProps.put("queue_type", "List");

        tester.setOperationContext(OperationType.EXECUTE, connProps, opProps, "Send", null);
    }

    @Test
    public void listModeCallsRpush() throws Exception {
        String payload = "{\"key\": \"q\", \"value\": \"v\"}";

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class)) {
            List<SimpleOperationResult> results = tester.executeExecuteOperation(
                    Collections.singletonList(new ByteArrayInputStream(payload.getBytes())));

            assertFalse(results.isEmpty());
            assertEquals(OperationStatus.SUCCESS, results.get(0).getStatus());
            verify(mocked.constructed().get(0)).rpush("q", "v");
        }
    }

    @Test
    public void streamModeCallsXadd() throws Exception {
        opProps.put("queue_type", "Stream");
        tester.setOperationContext(OperationType.EXECUTE, connProps, opProps, "Send", null);
        String payload = "{\"key\": \"s\", \"value\": \"v\"}";

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class)) {
            List<SimpleOperationResult> results = tester.executeExecuteOperation(
                    Collections.singletonList(new ByteArrayInputStream(payload.getBytes())));

            assertFalse(results.isEmpty());
            assertEquals(OperationStatus.SUCCESS, results.get(0).getStatus());
            verify(mocked.constructed().get(0)).xadd("s", "v");
        }
    }

    @Test
    public void missingKeyAddsFailureAndNeverPushes() throws Exception {
        String payload = "{\"value\": \"v\"}";

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class)) {
            List<SimpleOperationResult> results = tester.executeExecuteOperation(
                    Collections.singletonList(new ByteArrayInputStream(payload.getBytes())));

            assertFalse(results.isEmpty());
            assertEquals(OperationStatus.FAILURE, results.get(0).getStatus());
            verify(mocked.constructed().get(0), never()).rpush(anyString(), anyString());
        }
    }

    @Test
    public void missingValueAddsFailure() throws Exception {
        String payload = "{\"key\": \"q\"}";

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class)) {
            List<SimpleOperationResult> results = tester.executeExecuteOperation(
                    Collections.singletonList(new ByteArrayInputStream(payload.getBytes())));

            assertFalse(results.isEmpty());
            assertEquals(OperationStatus.FAILURE, results.get(0).getStatus());
        }
    }

    @Test
    public void connectionIsClosedAfterExecute() throws Exception {
        String payload = "{\"key\": \"q\", \"value\": \"v\"}";

        try (MockedConstruction<RedisConnection> mocked = mockConstruction(RedisConnection.class)) {
            tester.executeExecuteOperation(Collections.singletonList(new ByteArrayInputStream(payload.getBytes())));
            verify(mocked.constructed().get(0)).close();
        }
    }
}
