package com.boomi.connector.redis.connection;

import redis.clients.jedis.*;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.time.Duration;
import java.util.*;
import java.util.logging.Logger;

/**
 * Redis client implementation for Redis Cluster deployments.
 * Uses JedisCluster which handles connection pooling and cluster topology management internally.
 */
public class ClusteredRedisConnection extends BaseRedisConnection {
    
    private static final Logger logger = Logger.getLogger(ClusteredRedisConnection.class.getName());
    private static final int MAX_ATTEMPTS = 3;
    private JedisCluster jedisCluster;
    
    public ClusteredRedisConnection(RedisConnectionConfig config) {
        this(config, new DefaultJedisClientFactory());
    }

    ClusteredRedisConnection(RedisConnectionConfig config, JedisClientFactory clientFactory) {
        super(config, clientFactory);
        initializeCluster();
    }

    /**
     * Initializes the Redis cluster connection.
     */
    private void initializeCluster() {
        Set<HostAndPort> clusterNodes = config.getClusterNodes();

        jedisCluster = clientFactory.createCluster(
                clusterNodes,
                buildClientConfig(),
                MAX_ATTEMPTS,
                Duration.ofMillis(config.getSocketTimeout()),
                createConnectionPoolConfig());

        logger.info("Initialized Redis cluster connection with " + clusterNodes.size() + " nodes");
    }
    
    @Override
    public String get(String key) {
        return jedisCluster.get(key);
    }
    
    @Override
    public void set(String key, String value, Long ttl) {
        if (ttl != null && ttl != -1) {
            logger.info("Setting key with TTL: " + key + " and value: " + value + " TTL: " + ttl);
            jedisCluster.psetex(key, ttl, value);
        } else {
            logger.info("Setting key without TTL: " + key + " and value: " + value);
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
        // Delete one key per call: a multi-key DEL across hash slots fails with CROSSSLOT.
        for (String key : scanAllNodes(scanPattern)) {
            jedisCluster.del(key);
        }
    }

    @Override
    public Map<String, String> getAll(String pattern) {
        Map<String, String> result = new HashMap<>();
        String scanPattern = prepareScanPattern(pattern);
        logger.info("Cluster scanning with pattern: " + scanPattern);
        for (String key : scanAllNodes(scanPattern)) {
            String value = jedisCluster.get(key);
            if (value != null) {
                result.put(key, value);
            }
        }
        logger.info("Total keys found in cluster: " + result.size());
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
    
    @Override
    public void close() {
        if (jedisCluster != null) {
            try {
                jedisCluster.close();
                logger.info("Closed Redis cluster connection");
            } catch (Exception e) {
                logger.warning("Error closing Redis cluster connection: " + e.getMessage());
            }
            jedisCluster = null;
        }
    }
}