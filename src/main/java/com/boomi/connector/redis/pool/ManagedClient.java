package com.boomi.connector.redis.pool;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A shared Redis client ({@code JedisPool} or {@code JedisCluster}) tracked by
 * {@link RedisClientPoolManager}, together with its reference count and last-access time.
 * Mirrors the expiry state the JMS V2 connector keeps on {@code AdapterPoolImpl}.
 *
 * <p>The client is typed {@code AutoCloseable} rather than {@code java.io.Closeable} because
 * {@code JedisCluster} (via {@code UnifiedJedis}) implements only {@code AutoCloseable}, not the
 * narrower {@code Closeable}; {@code JedisPool} (via commons-pool2's {@code GenericObjectPool})
 * implements {@code Closeable}, which extends {@code AutoCloseable}, so it is accepted here too.
 */
class ManagedClient {

    private static final Logger LOG = Logger.getLogger(ManagedClient.class.getName());

    final AutoCloseable client;
    final AtomicInteger activeReferences = new AtomicInteger(1);
    volatile long lastAccessTimeMillis;
    volatile boolean closed;

    ManagedClient(AutoCloseable client, long nowMillis) {
        this.client = client;
        this.lastAccessTimeMillis = nowMillis;
    }

    void touch(long nowMillis) {
        lastAccessTimeMillis = nowMillis;
    }

    /**
     * A client is expired when no live connection instance references it and it has not been
     * acquired or released within the expiration interval. Mirrors AdapterPoolImpl.isExpired.
     */
    boolean isExpired(long nowMillis) {
        return activeReferences.get() <= 0
                && (nowMillis - lastAccessTimeMillis) > RedisClientPoolManager.CLIENT_EXPIRATION_INTERVAL_MILLIS;
    }

    void closeQuietly() {
        try {
            client.close();
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Error closing shared Redis client", e);
        }
    }
}
