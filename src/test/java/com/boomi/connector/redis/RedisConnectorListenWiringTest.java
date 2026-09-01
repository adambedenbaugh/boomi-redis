package com.boomi.connector.redis;

import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.listen.ListenConnector;
import com.boomi.connector.redis.pool.RedisListenManager;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class RedisConnectorListenWiringTest {

    @Test
    public void connectorIsAListenConnector() {
        assertTrue(new RedisConnector() instanceof ListenConnector);
    }

    @Test
    public void createListenManagerReturnsRedisListenManager() {
        RedisConnector connector = new RedisConnector();
        ConnectorContext ctx = mock(ConnectorContext.class);
        assertTrue(connector.createListenManager(ctx) instanceof RedisListenManager);
    }

    @Test
    public void listenManagerStartAndStopDoNotThrow() {
        RedisListenManager manager = new RedisListenManager();
        manager.start();
        manager.stop();
    }
}
