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
    
    // Token expiration buffer in milliseconds (5 minutes before actual expiration)
    protected static final long TOKEN_EXPIRATION_BUFFER_MS = 5 * 60 * 1000;
    
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
    
    // /**
    //  * Authenticates a Jedis connection if credentials are required.
    //  * 
    //  * @param jedis The Jedis connection to authenticate
    //  */
    // protected void authenticateConnection(Jedis jedis) {
    //     if (requiresAuth()) {
    //         if (username != null && password != null) {
    //             jedis.auth(username, password);
    //         } else if (password != null) {
    //             jedis.auth(password);
    //         }
    //     }
    // }
    
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
    
    // /**
    //  * Utility method to safely close a Jedis connection.
    //  * Handles exceptions gracefully to prevent resource leaks.
    //  * 
    //  * @param jedis The Jedis connection to close
    //  */
    // protected void closeJedisConnection(Jedis jedis) {
    //     if (jedis != null) {
    //         try {
    //             jedis.close();
    //         } catch (Exception e) {
    //             logger.warning("Error closing Jedis connection: " + e.getMessage());
    //             // Log error but don't propagate to avoid masking original exceptions
    //         }
    //     }
    // }
    
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
    
    // /**
    //  * Checks if the Microsoft Entra token is about to expire.
    //  * @return true if the token expires within the buffer time, false otherwise
    //  */
    // protected boolean isTokenAboutToExpire() {
    //     if (!authenticationType.isMicrosoftEntra() || microsoftEntraCredential == null) {
    //         return false;
    //     }
        
    //     long currentTime = System.currentTimeMillis();
    //     long tokenExpirationTime = microsoftEntraCredential.getExpiresAtMillis();
    //     long timeUntilExpiration = tokenExpirationTime - currentTime;
        
    //     boolean aboutToExpire = timeUntilExpiration <= TOKEN_EXPIRATION_BUFFER_MS;
    //     if (aboutToExpire) {
    //         logger.info("Microsoft Entra token is about to expire in " + 
    //                          (timeUntilExpiration / 1000) + " seconds. Refreshing proactively.");
    //     }
        
    //     return aboutToExpire;
    // }

    // /**
    //  * Refreshes authentication credentials for Microsoft Entra ID.
    //  * Updates the internal username and password fields with new token.
    //  */
    // protected void refreshEntraAuthentication() {
    //     if (authenticationType == AuthenticationType.MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL) {
    //         logger.info("Refreshing Microsoft Entra authentication...");
    //         try {
    //             String tenantId = config.getTenantId();
    //             String clientId = config.getClientId();
    //             String clientSecret = config.getClientSecret();
                
    //             if (tenantId == null || clientId == null || clientSecret == null) {
    //                 throw new IllegalArgumentException(
    //                     "Microsoft Entra authentication requires tenantId, clientId, and clientSecret"
    //                 );
    //             }
                
    //             MicrosoftEntraClientSecretCredential credential = 
    //                 new MicrosoftEntraClientSecretCredential(tenantId, clientId, clientSecret);
                    
    //             // Update stored credential and authentication fields
    //             this.microsoftEntraCredential = credential;
    //             this.username = credential.getUsername();
    //             this.password = credential.getToken();
    //             logger.info("Microsoft Entra authentication refreshed successfully");
                
    //             // Notify subclasses to update their connection pools
    //             onAuthenticationRefreshed(this.username, this.password);
                
    //         } catch (Exception e) {
    //             logger.severe("Failed to refresh Microsoft Entra authentication: " + e.getMessage());
    //             throw new RuntimeException("Failed to refresh Microsoft Entra authentication", e);
    //         }
    //     }
    // }
    


    /**
     * Tests the connection. Subclasses can override this method if they want to use retry logic.
     * Default implementation returns true (assumes connection is valid).
     * @return true if connection is valid, false otherwise
     */
    protected boolean testConnection() {
        return true;
    }

    // /**
    //  * Reinitializes the connection. Subclasses should override this method if they want to use retry logic.
    //  * Default implementation does nothing.
    //  */
    // protected void reinitializeConnection() {
    //     // Default implementation - subclasses can override if needed
    //     logger.info("Default reinitializeConnection - no action taken");
    // }

    // /**
    //  * Called when authentication credentials are refreshed.
    //  * Subclasses should override this to update their connection pools.
    //  * 
    //  * @param newUsername The new username
    //  * @param newPassword The new password/token
    //  */
    // protected void onAuthenticationRefreshed(String newUsername, String newPassword) {
    //     // Default implementation - subclasses should override
    //     logger.info("Authentication refreshed - subclass should handle connection pool update");
    // }
    
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