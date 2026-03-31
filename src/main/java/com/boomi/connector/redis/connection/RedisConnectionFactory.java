package com.boomi.connector.redis.connection;

import com.boomi.connector.api.BrowseContext;
import java.util.logging.Logger;

/**
 * Factory class for creating appropriate Redis connection implementations based on configuration.
 * Determines the correct connection type (standalone, pooled, or clustered) and returns
 * the appropriate implementation that implements the RedisConnectionInterface.
 */
public class RedisConnectionFactory {
    
    private static final Logger logger = Logger.getLogger(RedisConnectionFactory.class.getName());
    
    /**
     * Creates a Redis connection instance based on the configuration in the BrowseContext.
     * The factory analyzes the configuration to determine:
     * - Whether it's a cluster or standalone deployment
     * - Whether connection pooling is enabled
     * - Authentication requirements
     * 
     * @param context The BrowseContext containing connection properties
     * @return A RedisConnectionInterface instance configured for the specified Redis deployment
     * @throws IllegalArgumentException if configuration is invalid
     * @throws RuntimeException if connection initialization fails
     */
    public static RedisConnectionInterface createConnection(BrowseContext context) {
        if (context == null) {
            throw new IllegalArgumentException("BrowseContext cannot be null");
        }
        
        try {
            RedisConnectionConfig config = new RedisConnectionConfig(context);
            
            if (config.isCluster()) {
                logger.info("Creating clustered Redis connection");
                return new ClusteredRedisConnection(config);
            } else if (config.isPoolEnabled()) {
                logger.info("Creating standalone pooled Redis connection");
                return new StandalonePooledRedisConnection(config);
            } else {
                logger.info("Creating standalone Redis connection");
                return new StandaloneRedisConnection(config);
            }
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Redis connection: " + e.getMessage(), e);
        }
    }
    
    /**
     * Creates a Redis connection instance with explicit configuration.
     * This method is useful for testing or when you already have a configuration object.
     * 
     * @param config The Redis connection configuration
     * @return A RedisConnectionInterface instance configured for the specified Redis deployment
     * @throws IllegalArgumentException if configuration is invalid
     * @throws RuntimeException if connection initialization fails
     */
    public static RedisConnectionInterface createConnection(RedisConnectionConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("RedisConnectionConfig cannot be null");
        }
        
        try {
            if (config.isCluster()) {
                logger.info("Creating clustered Redis connection");
                return new ClusteredRedisConnection(config);
            } else if (config.isPoolEnabled()) {
                logger.info("Creating standalone pooled Redis connection");
                return new StandalonePooledRedisConnection(config);
            } else {
                logger.info("Creating standalone Redis connection");
                return new StandaloneRedisConnection(config);
            }
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Redis connection: " + e.getMessage(), e);
        }
    }
}