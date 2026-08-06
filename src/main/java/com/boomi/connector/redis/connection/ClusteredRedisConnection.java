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
        ScanParams scanParams = new ScanParams().count(100).match(scanPattern);
        String cursor = ScanParams.SCAN_POINTER_START;
        
        do {
            ScanResult<String> scanResult = jedisCluster.scan(cursor, scanParams);
            List<String> keys = scanResult.getResult();

            // Delete one key per call. A multi-key DEL whose keys span hash slots fails
            // with CROSSSLOT on an OSS cluster, so batched del(String[]) is not safe here.
            for (String key : keys) {
                jedisCluster.del(key);
            }

            cursor = scanResult.getCursor();
        } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
    }
    
    @Override
    public Map<String, String> getAll(String pattern) {
        Map<String, String> result = new HashMap<>();
        String scanPattern = prepareScanPattern(pattern);
        ScanParams scanParams = new ScanParams().count(100).match(scanPattern);
        String cursor = ScanParams.SCAN_POINTER_START;
        
        logger.info("Cluster scanning with pattern: " + scanPattern);
        
        do {
            ScanResult<String> scanResult = jedisCluster.scan(cursor, scanParams);
            List<String> keys = scanResult.getResult();
            
            // For clusters, we need individual gets due to key distribution across nodes
            for (String key : keys) {
                String value = jedisCluster.get(key);
                if (value != null) {
                    result.put(key, value);
                }
            }
            cursor = scanResult.getCursor();
        } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        
        logger.info("Total keys found in cluster: " + result.size());
        return result;
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