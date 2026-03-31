package com.boomi.connector.redis.connection;

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
 * Uses a JedisPool to manage connections efficiently for concurrent operations.
 * Includes automatic connection testing and retry logic with Microsoft Entra token refresh.
 */
public class StandalonePooledRedisConnection extends BaseRedisConnection {
    
    private static final Logger logger = Logger.getLogger(StandalonePooledRedisConnection.class.getName());
    
    // Shared pool management
    private static final Map<String, PoolInfo> sharedPools = new ConcurrentHashMap<>();
    private static final Object poolLock = new Object();
    
    // Instance fields
    private JedisPool jedisPool;
    private String poolKey;
    
    /**
     * Helper class to track pool instances and reference counts
     */
    private static class PoolInfo {
        final JedisPool pool;
        final AtomicInteger referenceCount;
        
        PoolInfo(JedisPool pool) {
            this.pool = pool;
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
        super(config);
        this.poolKey = generatePoolKey(config);
        initializePool();
    }
    
    /**
     * Generates a unique key for the pool based on connection configuration.
     */
    private String generatePoolKey(RedisConnectionConfig config) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(config.getHost()).append(":")
                  .append(config.getPort()).append(":")
                  .append(config.isSSLEnabled()).append(":")
                  .append(config.getSocketTimeout()).append(":")
                  .append(config.getPoolSize());
        
        if (requiresAuth()) {
            keyBuilder.append(":").append(getAuthUsername())
                      .append(":").append(Objects.hashCode(getAuthPassword()));
        }
        
        return keyBuilder.toString();
    }
    
    /**
     * Initializes the Jedis connection pool using shared pool management.
     * Multiple instances with the same configuration will share the same pool.
     */
    private void initializePool() {
        synchronized (poolLock) {
            PoolInfo poolInfo = sharedPools.get(poolKey);
            
            if (poolInfo != null && !poolInfo.pool.isClosed()) {
                // Test existing shared pool
                try (Jedis testJedis = poolInfo.pool.getResource()) {
                    testJedis.ping();
                    // Pool is valid, use it and increment reference count
                    jedisPool = poolInfo.pool;
                    poolInfo.incrementReference();
                    logger.info("Reusing existing shared pool (references: " + poolInfo.getReferenceCount() + ")");
                    return;
                } catch (Exception e) {
                    // Pool is invalid, remove it
                    logger.warning("Existing shared pool is invalid, creating new one: " + e.getMessage());
                    sharedPools.remove(poolKey);
                    try {
                        poolInfo.pool.close();
                    } catch (Exception closeEx) {
                        logger.warning("Error closing invalid pool: " + closeEx.getMessage());
                    }
                }
            }
            
            // Create new pool
            createNewSharedPool();
        }
    }
    
    /**
     * Creates a new shared pool and stores it in the shared pools map.
     */
    private void createNewSharedPool() {
        String host = config.getHost();
        int port = config.getPort();
        
        JedisPool newPool;
        if (requiresAuth()) {
            newPool = new JedisPool(
                createJedisPoolConfig(),
                host,
                port,
                config.getSocketTimeout(),
                getAuthUsername(),
                getAuthPassword(),
                config.isSSLEnabled()
            );
        } else {
            newPool = new JedisPool(
                createJedisPoolConfig(),
                host,
                port,
                config.getSocketTimeout(),
                config.isSSLEnabled()
            );
        }
        
        // Store in shared pools and set instance reference
        PoolInfo poolInfo = new PoolInfo(newPool);
        sharedPools.put(poolKey, poolInfo);
        jedisPool = newPool;
        
        logger.info("Created new shared Redis connection pool to " + host + ":" + port + 
                   " with pool size: " + config.getPoolSize() + " (pool key: " + poolKey + ")");
    }
    
    /**
     * Tests the connection pool by attempting to ping Redis.
     * @return true if connection is valid, false otherwise
     */
    @Override
    protected boolean testConnection() {
        if (jedisPool == null) {
            return false;
        }
        
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.ping();
            return true;
        } catch (Exception e) {
            logger.warning("Connection pool test failed: " + e.getMessage());
            return false;
        }
    }
    
    @Override
    public String get(String key) {
        testConnection();
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
                logger.info("Setting key with TTL: " + key + " and value: " + value + " TTL: " + ttl);
                jedis.psetex(key, ttl, value);
            } else {
                logger.info("Setting key without TTL: " + key + " and value: " + value);
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
                    String[] keysArray = keys.toArray(new String[0]);
                    jedis.del(keysArray);
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
            
            logger.info("Scanning with pattern: " + scanPattern);
            
            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                List<String> foundKeys = scanResult.getResult();
                allKeys.addAll(foundKeys);
                cursor = scanResult.getCursor();
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            
            logger.info("Total keys found: " + allKeys.size());
            
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
    
    @Override
    public void close() {
        if (jedisPool != null && poolKey != null) {
            synchronized (poolLock) {
                PoolInfo poolInfo = sharedPools.get(poolKey);
                if (poolInfo != null) {
                    int remaining = poolInfo.decrementReference();
                    logger.info("Released pool reference (remaining: " + remaining + ")");
                    
                    if (remaining <= 0) {
                        // Last reference, close the pool
                        sharedPools.remove(poolKey);
                        try {
                            poolInfo.pool.close();
                            logger.info("Closed shared Redis connection pool (no more references)");
                        } catch (Exception e) {
                            logger.warning("Error closing shared Redis connection pool: " + e.getMessage());
                        }
                    }
                }
            }
            jedisPool = null;
            poolKey = null;
        }
    }
    
    /**
     * Closes all shared pools. Should be called during application shutdown.
     */
    public static void closeAllSharedPools() {
        synchronized (poolLock) {
            for (Map.Entry<String, PoolInfo> entry : sharedPools.entrySet()) {
                try {
                    entry.getValue().pool.close();
                    logger.info("Closed shared pool: " + entry.getKey());
                } catch (Exception e) {
                    logger.warning("Error closing shared pool " + entry.getKey() + ": " + e.getMessage());
                }
            }
            sharedPools.clear();
            logger.info("All shared Redis connection pools closed");
        }
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
        synchronized (poolLock) {
            for (Map.Entry<String, PoolInfo> entry : sharedPools.entrySet()) {
                info.put(entry.getKey(), entry.getValue().getReferenceCount());
            }
        }
        return info;
    }
}