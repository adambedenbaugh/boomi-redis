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

    /** Pushes a value onto the tail of a Redis List (producer side of the List queue pattern). */
    void rpush(String key, String value);

    /** Pops from the head of a Redis List, blocking up to {@code timeoutSeconds}. Null on timeout. */
    String blpop(String key, int timeoutSeconds);

    /**
     * Idempotently creates a Stream consumer group starting from the beginning of the stream (ID
     * "0"), creating the stream itself if it does not exist. A no-op if the group already exists.
     */
    void xgroupCreate(String key, String groupName);

    /** Appends a value to a Stream (producer side of the Stream queue pattern), auto-generated ID. */
    void xadd(String key, String value);

    /**
     * Reads up to {@code count} never-before-delivered entries for this consumer group, blocking
     * up to {@code blockMillis}. Returns an empty list on timeout.
     */
    java.util.List<RedisStreamMessage> xreadGroup(String key, String groupName, String consumerName, int count,
            int blockMillis);

    /** Acknowledges one Stream entry, removing it from the consumer group's pending entries list. */
    void xack(String key, String groupName, String messageId);

    /**
     * Lists pending (unacknowledged) entries idle at least {@code idleMillis}, with their delivery
     * counts, up to {@code count} entries.
     */
    java.util.List<RedisStreamPendingMessage> xpendingIdle(String key, String groupName, long idleMillis, int count);

    /**
     * Claims ownership of one pending entry for {@code consumerName}, returning its id and value,
     * or null if it is no longer pending (already acknowledged or claimed by another consumer).
     */
    RedisStreamMessage xclaim(String key, String groupName, String consumerName, String messageId);
}