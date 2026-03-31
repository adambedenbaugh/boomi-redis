package com.boomi.connector.redis;

import java.util.*;
import java.util.logging.Logger;

import com.boomi.connector.api.BrowseContext;
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
                logger.info("RedisConnection initialized.");
            } catch (Exception e) {
                throwException(e);
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

    private void throwException(Throwable exception) {
        Utils.<RuntimeException>throwException(exception, null);
    }

    // Utility class for exception handling
    private static class Utils {
        @SuppressWarnings("unchecked")
        private static <T extends Throwable> void throwException(Throwable exception, Object dummy) throws T {
            throw (T) exception;
        }
    }
}