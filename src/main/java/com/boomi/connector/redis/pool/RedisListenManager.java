package com.boomi.connector.redis.pool;

import com.boomi.connector.api.listen.ListenManager;

/**
 * Connector-wide lifecycle hook required by {@link com.boomi.connector.api.listen.ListenConnector}.
 * Each {@code RedisListenOperation} owns its own {@code RedisConnection} and poll thread, so there
 * is no shared state to manage here yet - kept minimal per YAGNI. A natural home for anything that
 * later needs to be shared across concurrent Listen operation instances.
 */
public class RedisListenManager implements ListenManager {

    @Override
    public void start() {
        // No connector-wide shared resources to initialize.
    }

    @Override
    public void stop() {
        // No connector-wide shared resources to release.
    }
}
