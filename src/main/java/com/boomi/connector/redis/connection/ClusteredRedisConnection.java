package com.boomi.connector.redis.connection;

import redis.clients.jedis.*;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Redis client implementation for Redis Cluster deployments.
 * Uses JedisCluster which handles connection pooling and cluster topology management internally.
 *
 * <p>When pooling is enabled, the {@code JedisCluster} client (and its internal per-node pools) is
 * shared and reused across executions - keyed the same way as standalone pooling (see
 * {@link RedisConnectionConfig#getAuthIdentity()}) plus the cluster's node set - so repeated
 * executions against the same connection configuration skip topology re-discovery and reuse warm
 * per-node connections instead of paying for a fresh one every time. When pooling is disabled, a
 * private, unshared client is built fresh per execution and closed immediately after - matching
 * the standalone path's no-pooling semantics.
 */
public class ClusteredRedisConnection extends BaseRedisConnection {

    private static final Logger logger = Logger.getLogger(ClusteredRedisConnection.class.getName());
    private static final int MAX_ATTEMPTS = 3;

    // Shared cluster-client management (populated only when pooling is enabled)
    private static final Map<String, ClusterInfo> sharedClusters = new ConcurrentHashMap<>();
    private static final Map<String, Object> clusterLocks = new ConcurrentHashMap<>();

    private JedisCluster jedisCluster;
    private String clusterKey;
    private boolean shared;

    /** Tracks a shared cluster client and how many live instances currently reference it. */
    private static class ClusterInfo {
        final JedisCluster cluster;
        /** Sorted node set this client targets; used to evict superseded clients for the same cluster. */
        final String endpoint;
        final AtomicInteger referenceCount;
        volatile boolean closed;

        ClusterInfo(JedisCluster cluster, String endpoint) {
            this.cluster = cluster;
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

    public ClusteredRedisConnection(RedisConnectionConfig config) {
        this(config, new DefaultJedisClientFactory());
    }

    ClusteredRedisConnection(RedisConnectionConfig config, JedisClientFactory clientFactory) {
        super(config, clientFactory);
        initializeCluster();
    }

    /**
     * Initializes the Redis cluster connection. When pooling is enabled, reuses (or creates and
     * registers) a shared client keyed by the connection's identity so it persists across
     * executions. When pooling is disabled, builds a private, unshared client used by this
     * execution alone - matching the standalone "no pooling" behavior.
     */
    private void initializeCluster() {
        if (config.isPoolEnabled()) {
            shared = true;
            clusterKey = generateClusterKey(config);
            acquireSharedCluster();
        } else {
            shared = false;
            jedisCluster = buildNewCluster();
        }
    }

    /**
     * Reuses the existing shared client for this key if one is registered and still open;
     * otherwise builds a new one and registers it. No network I/O happens under the per-key lock;
     * liveness of a reused client is guarded by the pool's own testOnBorrow rather than an eager
     * ping here (mirroring StandalonePooledRedisConnection).
     */
    private void acquireSharedCluster() {
        boolean created = false;
        Object lock = clusterLocks.computeIfAbsent(clusterKey, k -> new Object());
        synchronized (lock) {
            ClusterInfo existing = sharedClusters.get(clusterKey);
            if (existing != null && !existing.closed) {
                existing.incrementReference();
                jedisCluster = existing.cluster;
            } else {
                JedisCluster newCluster = buildNewCluster();
                sharedClusters.put(clusterKey, new ClusterInfo(newCluster, endpoint()));
                jedisCluster = newCluster;
                created = true;
            }
        }
        if (created) {
            // Outside the new key's lock (never hold two per-key locks at once - that could
            // deadlock two concurrent creations evicting each other's endpoint).
            evictSupersededClusters(endpoint(), clusterKey);
        }
    }

    /** Sorted, comma-joined node set - the stable identity of the target cluster. */
    private String endpoint() {
        List<String> nodes = new ArrayList<>();
        for (HostAndPort node : config.getClusterNodes()) {
            nodes.add(node.toString());
        }
        Collections.sort(nodes);
        return String.join(",", nodes);
    }

    /**
     * Closes and removes shared cluster clients for the same node set registered under a different
     * (superseded) key - e.g. after a credential rotation or a timeout/pool-setting change produced
     * a new key. Without this, the old client would sit in the map forever with its per-node pools'
     * evictors keeping minIdle connections alive (and, once the old credential is revoked
     * server-side, re-attempting AUTH with dead credentials every eviction run). Entries still
     * referenced by an in-flight execution are skipped; the next client creation sweeps again.
     *
     * <p>Trade-off: two deliberately different connection configurations targeting the same node
     * set will evict each other's idle clients and degrade to a client rebuild per execution -
     * correct, just unshared. That rare case is accepted in exchange for cleaning up the common one
     * (a credential/setting change leaving a permanently stale client).
     */
    private static void evictSupersededClusters(String endpoint, String currentKey) {
        for (Map.Entry<String, ClusterInfo> entry : sharedClusters.entrySet()) {
            String key = entry.getKey();
            if (key.equals(currentKey) || !endpoint.equals(entry.getValue().endpoint)) {
                continue;
            }
            Object lock = clusterLocks.computeIfAbsent(key, k -> new Object());
            synchronized (lock) {
                ClusterInfo candidate = sharedClusters.get(key);
                // Re-check under the candidate's lock: acquires increment under this same lock,
                // so refcount 0 here means no live instance holds the client.
                if (candidate != null && endpoint.equals(candidate.endpoint)
                        && candidate.getReferenceCount() == 0) {
                    candidate.closed = true;
                    sharedClusters.remove(key);
                    try {
                        candidate.cluster.close();
                        logger.info("Closed superseded Redis cluster client for " + endpoint);
                    } catch (Exception e) {
                        logger.warning("Error closing superseded cluster client for " + endpoint + ": " + e.getMessage());
                    }
                }
            }
        }
    }

    private JedisCluster buildNewCluster() {
        Set<HostAndPort> clusterNodes = config.getClusterNodes();
        return clientFactory.createCluster(
                clusterNodes,
                buildClientConfig(),
                MAX_ATTEMPTS,
                Duration.ofMillis(config.getSocketTimeout()),
                createConnectionPoolConfig());
    }

    /**
     * Generates a token-free key identifying this cluster configuration. Mirrors
     * {@code StandalonePooledRedisConnection.generatePoolKey} - a rotating Entra token never
     * changes this key (so refresh reuses the client), but a stable credential or node-set change
     * does (so fixing a secret rebuilds the client, no Atom restart needed). Every configuration
     * value baked into the cached client or its per-node pools must appear here - otherwise
     * changing that field in Boomi would silently keep using a client built with the old value.
     */
    private String generateClusterKey(RedisConnectionConfig config) {
        return new StringBuilder()
                .append(endpoint()).append(":")
                .append(config.isSSLEnabled()).append(":")
                .append(config.getConnectionTimeout()).append(":")
                .append(config.getSocketTimeout()).append(":")
                .append(config.getClusterMaxTotal()).append(":")
                .append(config.getMinPoolSize()).append(":")
                .append(config.getMaxIdleTime()).append(":")
                .append(config.getMaxWaitTime()).append(":")
                .append(config.getAuthIdentity())
                .toString();
    }

    @Override
    public String get(String key) {
        return jedisCluster.get(key);
    }

    @Override
    public void set(String key, String value, Long ttl) {
        if (ttl != null && ttl != -1) {
            logger.fine("Setting key with TTL: " + key + " TTL: " + ttl);
            jedisCluster.psetex(key, ttl, value);
        } else {
            logger.fine("Setting key without TTL: " + key);
            jedisCluster.set(key, value);
        }
    }

    @Override
    public void del(String key) {
        jedisCluster.del(key);
    }

    @Override
    public void delAll(String pattern) {
        String scanPattern = prepareScanPattern(pattern);
        Set<String> keys = scanAllNodes(scanPattern);
        if (!keys.isEmpty()) {
            // ClusterPipeline batches each command to the correct master node by slot internally
            // (never a multi-key DEL, which would fail with CROSSSLOT across hash slots), cutting
            // this down from one round trip per key to one round trip per node. try-with-resources
            // is mandatory: sync() only drains responses - only close() returns the per-node
            // connections the pipeline borrowed from the cluster's pools.
            try (ClusterPipeline pipeline = jedisCluster.pipelined()) {
                for (String key : keys) {
                    pipeline.del(key);
                }
                pipeline.sync();
            }
        }
    }

    @Override
    public Map<String, String> getAll(String pattern) {
        Map<String, String> result = new HashMap<>();
        String scanPattern = prepareScanPattern(pattern);
        Set<String> keys = scanAllNodes(scanPattern);
        if (!keys.isEmpty()) {
            // try-with-resources is mandatory: sync() only drains responses - only close() returns
            // the per-node connections the pipeline borrowed from the cluster's pools.
            try (ClusterPipeline pipeline = jedisCluster.pipelined()) {
                Map<String, Response<String>> responses = new LinkedHashMap<>();
                for (String key : keys) {
                    responses.put(key, pipeline.get(key));
                }
                pipeline.sync();
                for (Map.Entry<String, Response<String>> entry : responses.entrySet()) {
                    String value = entry.getValue().get();
                    if (value != null) {
                        result.put(entry.getKey(), value);
                    }
                }
            }
        }
        return result;
    }

    /**
     * Enumerates keys matching the pattern across the whole cluster by SCANning each node
     * directly. JedisCluster.scan(cursor, MATCH) requires the MATCH pattern to contain a
     * {hash-tag} so it can route to a single slot; a prefix wildcard has none, so we scan
     * every node's keyspace instead and union the results. Keys dedupe naturally (a Set),
     * so any replica nodes returned by getClusterNodes() are harmless.
     */
    private Set<String> scanAllNodes(String scanPattern) {
        Set<String> keys = new HashSet<>();
        ScanParams scanParams = new ScanParams().count(100).match(scanPattern);
        for (ConnectionPool pool : jedisCluster.getClusterNodes().values()) {
            try (Connection conn = pool.getResource()) {
                Jedis node = clientFactory.createClientFromConnection(conn);
                String cursor = ScanParams.SCAN_POINTER_START;
                do {
                    ScanResult<String> scanResult = node.scan(cursor, scanParams);
                    keys.addAll(scanResult.getResult());
                    cursor = scanResult.getCursor();
                } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            }
        }
        return keys;
    }

    @Override
    public boolean isValid() {
        try {
            // Test cluster connectivity by attempting a simple operation
            jedisCluster.ping();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Releases this instance's handle. When pooling is enabled, this only releases the shared
     * reference (mirroring {@code StandalonePooledRedisConnection}) - the shared client persists
     * across executions and is only actually closed by {@link #closeAllSharedClusters()}. When
     * pooling is disabled, this instance's client is private and unshared, so it is closed
     * immediately, exactly as before.
     */
    @Override
    public void close() {
        if (jedisCluster == null) {
            return;
        }
        if (shared) {
            ClusterInfo info = sharedClusters.get(clusterKey);
            // Only release a reference to the cluster this instance actually holds. If
            // closeAllSharedClusters() ran and a new client was registered under the same key in
            // the meantime, info.cluster would be that unrelated newer client, not this one.
            if (info != null && info.cluster == jedisCluster) {
                int remaining = info.decrementReference();
                logger.fine("Released shared cluster client reference (active: " + remaining + ")");
            }
        } else {
            try {
                jedisCluster.close();
                logger.fine("Closed unshared Redis cluster connection");
            } catch (Exception e) {
                logger.warning("Error closing Redis cluster connection: " + e.getMessage());
            }
        }
        jedisCluster = null;
        clusterKey = null;
    }

    /**
     * Closes all shared cluster clients. Should be called during application shutdown or test
     * teardown to isolate static state between tests. Contract: callers must be effectively
     * single-threaded (no concurrent client acquisition) - this method deliberately does not take
     * the per-key locks, so a racing acquire could register a client that the trailing clear()
     * orphans. Acceptable only because shutdown and sequential test fixtures are the only callers.
     */
    public static void closeAllSharedClusters() {
        for (Map.Entry<String, ClusterInfo> entry : sharedClusters.entrySet()) {
            ClusterInfo info = entry.getValue();
            info.closed = true;
            try {
                info.cluster.close();
                logger.info("Closed shared cluster client: " + entry.getKey());
            } catch (Exception e) {
                logger.warning("Error closing shared cluster client " + entry.getKey() + ": " + e.getMessage());
            }
        }
        sharedClusters.clear();
        clusterLocks.clear();
        logger.info("All shared Redis cluster clients closed");
    }

    /**
     * Gets the current number of shared cluster clients. Useful for monitoring and debugging.
     */
    public static int getSharedClusterCount() {
        return sharedClusters.size();
    }

    /**
     * Gets the active reference count for each shared cluster client, keyed by cluster key.
     * Useful for monitoring and debugging (e.g. confirming pooling is actually being reused).
     */
    public static Map<String, Integer> getSharedClusterInfo() {
        Map<String, Integer> info = new HashMap<>();
        for (Map.Entry<String, ClusterInfo> entry : sharedClusters.entrySet()) {
            info.put(entry.getKey(), entry.getValue().getReferenceCount());
        }
        return info;
    }
}
