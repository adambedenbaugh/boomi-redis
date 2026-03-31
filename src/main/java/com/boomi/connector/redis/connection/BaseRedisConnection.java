package com.boomi.connector.redis.connection;

import com.boomi.connector.redis.authentication.AuthenticationType;
import com.boomi.connector.redis.authentication.MicrosoftEntraClientSecretCredential;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.logging.Logger;

/**
 * Base class for Redis connection implementations.
 * Provides common functionality and utilities shared across different connection types.
 * Includes integrated authentication handling for all supported authentication types.
 */
public abstract class BaseRedisConnection implements RedisConnectionInterface {
    
    private static final Logger logger = Logger.getLogger(BaseRedisConnection.class.getName());
    protected final RedisConnectionConfig config;
    protected String username;
    protected String password;
    protected final AuthenticationType authenticationType;
    protected MicrosoftEntraClientSecretCredential microsoftEntraCredential;

    protected BaseRedisConnection(RedisConnectionConfig config) {
        this.config = config;
        this.authenticationType = config.getAuthenticationType();
        
        // Initialize authentication credentials based on type
        switch (authenticationType) {
            case NONE:
                this.username = null;
                this.password = null;
                break;
                
            case BASIC:
                this.username = config.getUsername();
                this.password = config.getPassword();
                break;
                
            case MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL:
                String tenantId = config.getTenantId();
                String clientId = config.getClientId();
                String clientSecret = config.getClientSecret();
                
                if (tenantId == null || clientId == null || clientSecret == null) {
                    throw new IllegalArgumentException(
                        "Microsoft Entra authentication requires tenantId, clientId, and clientSecret"
                    );
                }
                
                MicrosoftEntraClientSecretCredential credential = 
                    new MicrosoftEntraClientSecretCredential(tenantId, clientId, clientSecret);
                
                // Store credential for token expiration checking
                this.microsoftEntraCredential = credential;
                this.username = credential.getUsername();
                this.password = credential.getToken();
                break;
                
            default:
                throw new IllegalArgumentException("Unsupported authentication type: " + authenticationType);
        }
    }
    
    /**
     * Creates a generic object pool configuration for Jedis connections.
     * Used by pooled connection implementations.
     * 
     * @return Configured GenericObjectPoolConfig for Jedis
     */
    protected GenericObjectPoolConfig<Jedis> createJedisPoolConfig() {
        GenericObjectPoolConfig<Jedis> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxWait(Duration.ofSeconds(config.getMaxWaitTime()));
        poolConfig.setMaxTotal(config.getPoolSize());
        poolConfig.setMinIdle(config.getMinPoolSize());
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleDuration(Duration.ofSeconds(config.getMaxIdleTime()));
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30));
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        
        return poolConfig;
    }
    
    /**
     * Creates a generic object pool configuration for cluster connections.
     * Used by cluster connection implementations.
     * 
     * @return Configured GenericObjectPoolConfig for Connection
     */
    protected GenericObjectPoolConfig<Connection> createConnectionPoolConfig() {
        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxWait(Duration.ofSeconds(config.getMaxWaitTime()));
        poolConfig.setMaxTotal(config.getPoolSize());
        poolConfig.setMinIdle(config.getMinPoolSize());
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleDuration(Duration.ofSeconds(config.getMaxIdleTime()));
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30));
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        
        logger.info("Created Jedis Cluster Pool with size: " + config.getPoolSize());
        return poolConfig;
    }
    
    /**
     * Determines if authentication is required based on the type.
     * 
     * @return true if authentication credentials are needed
     */
    protected boolean requiresAuth() {
        return authenticationType.requiresCredentials();
    }
    
    /**
     * Gets the username for authentication.
     * 
     * @return The username, or null if not applicable
     */
    protected String getAuthUsername() {
        return username;
    }
    
    /**
     * Gets the password/token for authentication.
     * 
     * @return The password or token, or null if not applicable
     */
    protected String getAuthPassword() {
        return password;
    }
    
    /**
     * Validates that a pattern is suitable for SCAN operations.
     * Adds wildcards if necessary to ensure proper pattern matching.
     * 
     * @param pattern The original pattern
     * @return A pattern suitable for SCAN operations
     */
    protected String prepareScanPattern(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return "*";
        }
        
        // If no wildcards present, add * to find keys with this prefix
        if (!pattern.contains("*") && !pattern.contains("?")) {
            return pattern + "*";
        }
        
        return pattern;
    }
    
    /**
     * Tests the connection. Subclasses can override this method if they want to use retry logic.
     * Default implementation returns true (assumes connection is valid).
     * @return true if connection is valid, false otherwise
     */
    protected boolean testConnection() {
        return true;
    }

    /**
     * Gets the current authentication username.
     * Subclasses can override to return updated credentials.
     * 
     * @return The current username
     */
    protected String getCurrentUsername() {
        return username;
    }
    
    /**
     * Gets the current authentication password/token.
     * Subclasses can override to return updated credentials.
     * 
     * @return The current password/token
     */
    protected String getCurrentPassword() {
        return password;
    }

    @Override
    public boolean isValid() {
        try {
            // Default implementation - subclasses can override for specific validation logic
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}