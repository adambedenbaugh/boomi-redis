package com.boomi.connector.redis.connection;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Redis client implementation for standalone (single-node) Redis instances with connection pooling.
 * Uses a JedisPool to manage connections efficiently across many operation executions.
 * Pools are shared by a token-free identity key (host/port/ssl/timeout/pool-size/auth-identity) so
 * that a rotating Entra token never orphans a pool. Access to a given pool key is serialized via a
 * per-key lock so unrelated pool keys never contend with each other.
 *
 * <p>A shared pool outlives any single instance: {@link #close()} releases only this instance's
 * handle to it (and updates the diagnostic active-reference count), it never tears down the pool
 * itself, so the next execution against the same connection configuration reuses warm connections
 * instead of paying a fresh connect/AUTH/TLS handshake. Pools are only actually closed via
 * {@link #closeAllSharedPools()}.
 */
public class StandalonePooledRedisConnection extends BaseRedisConnection {

    private static final Logger logger = Logger.getLogger(StandalonePooledRedisConnection.class.getName());

    // Shared pool management
    private static final Map<String, PoolInfo> sharedPools = new ConcurrentHashMap<>();
    private static final Map<String, Object> poolLocks = new ConcurrentHashMap<>();

    // Instance fields
    private JedisPool jedisPool;
    private String poolKey;

    /**
     * Helper class to track pool instances, their endpoint, and reference counts
     */
    private static class PoolInfo {
        final JedisPool pool;
        /** host:port this pool connects to; used to evict superseded pools for the same endpoint. */
        final String endpoint;
        final AtomicInteger referenceCount;

        PoolInfo(JedisPool pool, String endpoint) {
            this.pool = pool;
            this.endpoint = endpoint;
            this.referenceCount = new AtomicInteger(1);
        }

        void incrementReference() {
            referenceCount.incrementAndGet();
        }

        int decrementReference() {
            return referenceCount.decrementAndGet();
        }

        int getReferenceCount() {
            return referenceCount.get();
        }
    }

    public StandalonePooledRedisConnection(RedisConnectionConfig config) {
        this(config, new DefaultJedisClientFactory());
    }

    StandalonePooledRedisConnection(RedisConnectionConfig config, JedisClientFactory clientFactory) {
        super(config, clientFactory);
        this.poolKey = generatePoolKey(config);
        initializePool();
    }

    /**
     * Generates a unique, token-free key for the pool based on connection configuration. Every
     * configuration value that is baked into the pool or its client config must appear here -
     * otherwise changing that field in Boomi would silently keep using a pool built with the old
     * value (pools are never closed by instances, so there is no eventual pickup).
     */
    private String generatePoolKey(RedisConnectionConfig config) {
        return new StringBuilder()
                .append(config.getHost()).append(":")
                .append(config.getPort()).append(":")
                .append(config.isSSLEnabled()).append(":")
                .append(config.getConnectionTimeout()).append(":")
                .append(config.getSocketTimeout()).append(":")
                .append(config.getPoolSize()).append(":")
                .append(config.getMinPoolSize()).append(":")
                .append(config.getMaxIdleTime()).append(":")
                .append(config.getMaxWaitTime()).append(":")
                .append(config.getAuthIdentity())
                .toString();
    }

    /**
     * Initializes the Jedis connection pool using shared pool management.
     * Multiple instances with the same configuration will share the same pool.
     * No network I/O happens under the per-key lock; liveness of a reused pool
     * is guarded by testOnBorrow rather than an eager ping here.
     */
    private void initializePool() {
        boolean created = false;
        Object lock = poolLocks.computeIfAbsent(poolKey, k -> new Object());
        synchronized (lock) {
            PoolInfo existing = sharedPools.get(poolKey);
            if (existing != null && !existing.pool.isClosed()) {
                existing.incrementReference();
                jedisPool = existing.pool;
            } else {
                createNewSharedPool();
                created = true;
            }
        }
        if (created) {
            // Outside the new key's lock (never hold two per-key locks at once - that could
            // deadlock two concurrent creations evicting each other's endpoint).
            evictSupersededPools(endpoint(), poolKey);
        }
    }

    private String endpoint() {
        return config.getHost() + ":" + config.getPort();
    }

    /**
     * Creates a new shared pool and stores it in the shared pools map.
     */
    private void createNewSharedPool() {
        HostAndPort node = new HostAndPort(config.getHost(), config.getPort());
        JedisPool newPool = clientFactory.createPool(createJedisPoolConfig(), node, buildClientConfig());
        sharedPools.put(poolKey, new PoolInfo(newPool, endpoint()));
        jedisPool = newPool;

        logger.info("Created new shared Redis connection pool to " + node
                + " with pool size: " + config.getPoolSize());
    }

    /**
     * Closes and removes pools for the same endpoint registered under a different (superseded)
     * key - e.g. after a credential rotation or a pool-setting change produced a new key. Without
     * this, the old pool would sit in the map forever with its evictor keeping minIdle connections
     * alive (and, once the old credential is revoked server-side, re-attempting AUTH with dead
     * credentials every eviction run). Entries still referenced by an in-flight execution are
     * skipped; the next pool creation sweeps again.
     *
     * <p>Trade-off: two deliberately different connection configurations targeting the same
     * endpoint (e.g. two ACL users) will evict each other's idle pools and degrade to a pool
     * rebuild per execution - correct, just unpooled. That rare case is accepted in exchange for
     * cleaning up the common one (a credential/setting change leaving a permanently stale pool).
     */
    private static void evictSupersededPools(String endpoint, String currentKey) {
        for (Map.Entry<String, PoolInfo> entry : sharedPools.entrySet()) {
            String key = entry.getKey();
            if (key.equals(currentKey) || !endpoint.equals(entry.getValue().endpoint)) {
                continue;
            }
            Object lock = poolLocks.computeIfAbsent(key, k -> new Object());
            synchronized (lock) {
                PoolInfo candidate = sharedPools.get(key);
                // Re-check under the candidate's lock: acquires increment under this same lock,
                // so refcount 0 here means no live instance holds the pool.
                if (candidate != null && endpoint.equals(candidate.endpoint)
                        && candidate.getReferenceCount() == 0) {
                    sharedPools.remove(key);
                    try {
                        candidate.pool.close();
                        logger.info("Closed superseded Redis connection pool for " + endpoint);
                    } catch (Exception e) {
                        logger.warning("Error closing superseded pool for " + endpoint + ": " + e.getMessage());
                    }
                }
            }
        }
    }

    @Override
    public String get(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        } catch (Exception e) {
            logger.warning("Error getting key " + key + ": " + e.getMessage());
            throw e;
        }
    }

    @Override
    public void set(String key, String value, Long ttl) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (ttl != null && ttl != -1) {
                logger.fine("Setting key with TTL: " + key + " TTL: " + ttl);
                jedis.psetex(key, ttl, value);
            } else {
                logger.fine("Setting key without TTL: " + key);
                jedis.set(key, value);
            }
        } catch (Exception e) {
            logger.warning("Error setting key " + key + ": " + e.getMessage());
            throw e;
        }
    }

    @Override
    public void del(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(key);
        } catch (Exception e) {
            logger.warning("Error deleting key " + key + ": " + e.getMessage());
            throw e;
        }
    }

    @Override
    public void delAll(String pattern) {
        try (Jedis jedis = jedisPool.getResource()) {
            String scanPattern = prepareScanPattern(pattern);
            ScanParams scanParams = new ScanParams().count(100).match(scanPattern);
            String cursor = ScanParams.SCAN_POINTER_START;

            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                List<String> keys = scanResult.getResult();

                if (!keys.isEmpty()) {
                    // Pipelined single-key DELs; never a cross-slot multi-key DEL.
                    Pipeline pipeline = jedis.pipelined();
                    for (String key : keys) {
                        pipeline.del(key);
                    }
                    pipeline.sync();
                }

                cursor = scanResult.getCursor();
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        } catch (Exception e) {
            logger.warning("Error deleting keys with pattern " + pattern + ": " + e.getMessage());
            throw e;
        }
    }

    @Override
    public Map<String, String> getAll(String pattern) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> result = new HashMap<>();
            String scanPattern = prepareScanPattern(pattern);
            ScanParams scanParams = new ScanParams().match(scanPattern).count(1000);
            String cursor = ScanParams.SCAN_POINTER_START;
            List<String> allKeys = new ArrayList<>();

            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                List<String> foundKeys = scanResult.getResult();
                allKeys.addAll(foundKeys);
                cursor = scanResult.getCursor();
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));

            if (!allKeys.isEmpty()) {
                Pipeline pipeline = jedis.pipelined();
                List<Response<String>> responses = new ArrayList<>();

                for (String key : allKeys) {
                    responses.add(pipeline.get(key));
                }

                pipeline.sync();

                // Collect results
                for (int i = 0; i < allKeys.size(); i++) {
                    String key = allKeys.get(i);
                    String value = responses.get(i).get();
                    if (value != null) {
                        result.put(key, value);
                    }
                }
            }

            return result;
        } catch (Exception e) {
            logger.warning("Error getting all keys with pattern " + pattern + ": " + e.getMessage());
            throw e;
        }
    }

    @Override
    public boolean isValid() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.ping();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Releases this instance's handle to the shared pool. The pool itself is a long-lived resource
     * meant to be reused across many operation executions (that is the point of enabling pooling), so
     * this deliberately does NOT close or remove it from {@link #sharedPools} - only
     * {@link #closeAllSharedPools()} does that. This only updates the diagnostic active-reference
     * count, guarding against acting on a pool this instance no longer actually holds (e.g. if
     * {@link #closeAllSharedPools()} ran and a new pool was registered under the same key since this
     * instance was constructed).
     */
    @Override
    public void close() {
        if (jedisPool != null && poolKey != null) {
            PoolInfo poolInfo = sharedPools.get(poolKey);
            if (poolInfo != null && poolInfo.pool == jedisPool) {
                int remaining = poolInfo.decrementReference();
                logger.fine("Released pool reference (active: " + remaining + ")");
            }
            jedisPool = null;
            poolKey = null;
        }
    }

    /**
     * Closes all shared pools. Should be called during application shutdown or test teardown.
     * Contract: callers must be effectively single-threaded (no concurrent pool acquisition) -
     * this method deliberately does not take the per-key locks, so a racing acquire could register
     * a pool that the trailing clear() orphans, and clearing poolLocks lets two threads briefly
     * hold different lock objects for one key. Both are acceptable only because shutdown and
     * sequential test fixtures are the only callers.
     */
    public static void closeAllSharedPools() {
        for (Map.Entry<String, PoolInfo> entry : sharedPools.entrySet()) {
            try {
                entry.getValue().pool.close();
                logger.info("Closed shared pool: " + entry.getKey());
            } catch (Exception e) {
                logger.warning("Error closing shared pool " + entry.getKey() + ": " + e.getMessage());
            }
        }
        sharedPools.clear();
        poolLocks.clear();
        logger.info("All shared Redis connection pools closed");
    }

    /**
     * Gets the current number of shared pools.
     * Useful for monitoring and debugging.
     */
    public static int getSharedPoolCount() {
        return sharedPools.size();
    }

    /**
     * Gets information about all shared pools.
     * Useful for monitoring and debugging.
     */
    public static Map<String, Integer> getSharedPoolInfo() {
        Map<String, Integer> info = new HashMap<>();
        for (Map.Entry<String, PoolInfo> entry : sharedPools.entrySet()) {
            info.put(entry.getKey(), entry.getValue().getReferenceCount());
        }
        return info;
    }
}
