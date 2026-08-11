package com.boomi.connector.redis;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.redis.connection.RedisConnectionInterface;
import com.boomi.connector.redis.connection.RedisConnectionFactory;
import com.boomi.connector.util.BaseConnection;

public class RedisConnection extends BaseConnection<BrowseContext> {

    private static final Logger logger = Logger.getLogger(RedisConnection.class.getName());

    private RedisConnectionInterface redisClient;

    public RedisConnection(BrowseContext context) {
        super(context);
    }

    public void init() {
        if (redisClient == null) {
            try {
                redisClient = RedisConnectionFactory.createConnection(getContext());
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to initialize the Redis connection", e);
                throw new ConnectorException("Could not initialize the Redis connection. Verify the "
                        + "connection's Hosts, Clustering Policy, and Authentication settings. Cause: "
                        + e.getMessage(), e);
            }
        }
    }


    public boolean isValid() {
        init();
        return redisClient.isValid();
    }

    public Map<String, String> getAll(String pattern) {
        init();
        return redisClient.getAll(pattern);
    }
    
    public String get(String key) {
        init();
        return redisClient.get(key);
    }

    public void set(String key, String value, Long ttl) {
        init();
        redisClient.set(key, value, ttl);
    }

    public void del(String key) {
        init();
        redisClient.del(key);
    }

    public void delAll(String pattern) {
        init();
        redisClient.delAll(pattern);
    }

    /**
     * Closes the Redis connection and releases resources.
     */
    public void close() {
        if (redisClient != null) {
            redisClient.close();
            redisClient = null;
        }
    }
}