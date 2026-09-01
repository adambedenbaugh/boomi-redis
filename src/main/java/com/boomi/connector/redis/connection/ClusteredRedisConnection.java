package com.boomi.connector.redis.connection;

import com.boomi.connector.redis.pool.RedisClientPoolManager;
import com.boomi.connector.redis.pool.RedisClientSettings;
import redis.clients.jedis.*;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.time.Duration;
import java.util.*;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * Redis client implementation for Redis Cluster deployments.
 * Uses JedisCluster which handles connection pooling and cluster topology management internally.
 *
 * <p>When pooling is enabled, the {@code JedisCluster} client (and its internal per-node pools) is
 * owned by {@link RedisClientPoolManager}, keyed by {@link RedisClientSettings} (every connection
 * field), shared and reused across executions - so repeated executions against
 * the same connection configuration skip topology re-discovery and reuse warm per-node connections
 * instead of paying for a fresh one every time. It is closed only by the manager's idle eviction or
 * {@link RedisClientPoolManager#closeAll()} - never by this class. When pooling is disabled, a
 * private, unshared client is built fresh per execution and closed immediately after - matching
 * the standalone path's no-pooling semantics.
 */
public class ClusteredRedisConnection extends BaseRedisConnection {

    private static final Logger logger = Logger.getLogger(ClusteredRedisConnection.class.getName());
    private static final int MAX_ATTEMPTS = 3;

    private JedisCluster jedisCluster;
    private RedisClientSettings settings;
    private boolean shared;

    public ClusteredRedisConnection(RedisConnectionConfig config) {
        this(config, new DefaultJedisClientFactory());
    }

    ClusteredRedisConnection(RedisConnectionConfig config, JedisClientFactory clientFactory) {
        super(config, clientFactory);
        initializeCluster();
    }

    /**
     * When pooling is enabled, acquires the shared client from {@link RedisClientPoolManager}
     * (built once, reused across executions, closed by idle eviction). When pooling is disabled,
     * builds a private, unshared client used by this execution alone and closed in
     * {@link #close()} - matching the standalone path's no-pooling semantics.
     */
    private void initializeCluster() {
        if (config.isPoolEnabled()) {
            shared = true;
            settings = new RedisClientSettings(config);
            jedisCluster = RedisClientPoolManager.acquire(settings, new Supplier<JedisCluster>() {
                @Override
                public JedisCluster get() {
                    return buildNewCluster();
                }
            });
        } else {
            shared = false;
            jedisCluster = buildNewCluster();
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
     * Pooled: releases this instance's reference via the manager (the shared client persists for
     * the next execution; only idle eviction or closeAll() closes it). Unpooled: this instance's
     * client is private, so it is closed immediately.
     */
    @Override
    public void close() {
        if (jedisCluster == null) {
            return;
        }
        if (shared) {
            RedisClientPoolManager.release(settings, jedisCluster);
        } else {
            try {
                jedisCluster.close();
                logger.fine("Closed unshared Redis cluster connection");
            } catch (Exception e) {
                logger.warning("Error closing Redis cluster connection: " + e.getMessage());
            }
        }
        jedisCluster = null;
        settings = null;
    }

    @Override
    public void rpush(String key, String value) {
        jedisCluster.rpush(key, value);
    }

    @Override
    public String blpop(String key, int timeoutSeconds) {
        List<String> result = jedisCluster.blpop(timeoutSeconds, key);
        return result == null ? null : result.get(1);
    }

    @Override
    public void xgroupCreate(String key, String groupName) {
        try {
            jedisCluster.xgroupCreate(key, groupName, new StreamEntryID(0), true);
        } catch (redis.clients.jedis.exceptions.JedisDataException e) {
            if (e.getMessage() == null || !e.getMessage().contains("BUSYGROUP")) {
                throw e;
            }
        }
    }

    @Override
    public void xadd(String key, String value) {
        jedisCluster.xadd(key, StreamEntryID.NEW_ENTRY, Collections.singletonMap("value", value));
    }

    @Override
    public List<RedisStreamMessage> xreadGroup(String key, String groupName, String consumerName, int count,
            int blockMillis) {
        redis.clients.jedis.params.XReadGroupParams params =
                new redis.clients.jedis.params.XReadGroupParams().count(count).block(blockMillis);
        Map<String, StreamEntryID> streams = Collections.singletonMap(key, StreamEntryID.UNRECEIVED_ENTRY);
        return toStreamMessages(jedisCluster.xreadGroup(groupName, consumerName, params, streams));
    }

    @Override
    public void xack(String key, String groupName, String messageId) {
        jedisCluster.xack(key, groupName, new StreamEntryID(messageId));
    }

    @Override
    public List<RedisStreamPendingMessage> xpendingIdle(String key, String groupName, long idleMillis, int count) {
        redis.clients.jedis.params.XPendingParams params =
                redis.clients.jedis.params.XPendingParams.xPendingParams().idle(idleMillis).count(count);
        List<RedisStreamPendingMessage> result = new ArrayList<>();
        for (redis.clients.jedis.resps.StreamPendingEntry entry : jedisCluster.xpending(key, groupName, params)) {
            result.add(new RedisStreamPendingMessage(entry.getID().toString(), entry.getDeliveredTimes()));
        }
        return result;
    }

    @Override
    public RedisStreamMessage xclaim(String key, String groupName, String consumerName, String messageId) {
        List<redis.clients.jedis.resps.StreamEntry> claimed = jedisCluster.xclaim(key, groupName, consumerName, 0L,
                redis.clients.jedis.params.XClaimParams.xClaimParams(), new StreamEntryID(messageId));
        if (claimed.isEmpty() || claimed.get(0) == null) {
            return null;
        }
        redis.clients.jedis.resps.StreamEntry entry = claimed.get(0);
        return new RedisStreamMessage(entry.getID().toString(), entry.getFields().get("value"));
    }
}
