package com.boomi.connector.redis.connection;

import java.util.Map;

/**
 * Interface defining the contract for Redis connection implementations.
 * Provides a common API for Redis operations regardless of the underlying
 * connection type (standalone, pooled, or clustered).
 */
public interface RedisConnectionInterface {
    
    /**
     * Retrieves the value associated with the specified key.
     * 
     * @param key The key to retrieve
     * @return The value associated with the key, or null if key doesn't exist
     */
    String get(String key);
    
    /**
     * Sets the value for the specified key.
     * 
     * @param key The key to set
     * @param value The value to associate with the key
     * @param ttl Time-to-live in milliseconds, or null for no expiration
     */
    void set(String key, String value, Long ttl);
    
    /**
     * Deletes the specified key.
     * 
     * @param key The key to delete
     */
    void del(String key);
    
    /**
     * Deletes all keys matching the specified pattern.
     * 
     * @param pattern The pattern to match (supports wildcards)
     */
    void delAll(String pattern);
    
    /**
     * Retrieves all key-value pairs matching the specified pattern.
     * 
     * @param pattern The pattern to match (supports wildcards)
     * @return Map of key-value pairs matching the pattern
     */
    Map<String, String> getAll(String pattern);
    
    /**
     * Checks if the connection is valid and operational.
     * 
     * @return true if the connection is valid, false otherwise
     */
    boolean isValid();
    
    /**
     * Closes the Redis connection and releases any resources.
     * This method should be called when the client is no longer needed.
     */
    void close();
}