package com.boomi.connector.redis.connection;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.*;
import java.util.logging.Logger;

/**
 * Redis client implementation for standalone (single-node) Redis instances without connection pooling.
 * Uses a direct Jedis connection for all operations.
 */
public class StandaloneRedisConnection extends BaseRedisConnection {
    
    private static final Logger logger = Logger.getLogger(StandaloneRedisConnection.class.getName());
    private Jedis jedis;
    
    public StandaloneRedisConnection(RedisConnectionConfig config) {
        super(config);
        initializeConnection();
    }
    
    private void initializeConnection() {
        String host = config.getHost();
        int port = config.getPort();
        
        jedis = new Jedis(host, port, config.getSocketTimeout(), config.isSSLEnabled());
        if (requiresAuth()) {
            if (username != null && password != null) {
                jedis.auth(username, password);
            } else if (password != null) {
                jedis.auth(password);
            }
        }
        
        logger.info("Initialized standalone Redis connection to " + host + ":" + port);
    }
    
    @Override
    public String get(String key) {
        return jedis.get(key);
    }
    
    @Override
    public void set(String key, String value, Long ttl) {
        if (ttl != null && ttl != -1) {
            logger.fine("Setting key with TTL: " + key + " and value: " + value + " TTL: " + ttl);
            jedis.psetex(key, ttl, value);
        } else {
            logger.fine("Setting key without TTL: " + key + " and value: " + value);
            jedis.set(key, value);
        }
    }
    
    @Override
    public void del(String key) {
        jedis.del(key);
    }
    
    @Override
    public void delAll(String pattern) {
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
    }
    
    @Override
    public Map<String, String> getAll(String pattern) {
        Map<String, String> result = new HashMap<>();
        String scanPattern = prepareScanPattern(pattern);
        ScanParams scanParams = new ScanParams().match(scanPattern).count(1000);
        String cursor = ScanParams.SCAN_POINTER_START;
        List<String> allKeys = new ArrayList<>();
        
        logger.info("Scanning with pattern: " + scanPattern);
        
        // Collect all matching keys using SCAN
        do {
            ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
            List<String> foundKeys = scanResult.getResult();
            allKeys.addAll(foundKeys);
            cursor = scanResult.getCursor();
        } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        
        logger.info("Total keys found: " + allKeys.size());
        
        // Get all values
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
    }
    
    @Override
    public boolean isValid() {
        try {
            jedis.ping();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void close() {
        if (jedis != null) {
            try {
                jedis.close();
            } catch (Exception e) {
                logger.warning("Error closing Jedis connection: " + e.getMessage());
            }
        }
        jedis = null;
    }
}