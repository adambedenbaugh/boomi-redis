package com.boomi.connector.redis.connection;

import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.OAuth2Token;
import com.boomi.connector.redis.authentication.AuthenticationType;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
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
                OAuth2Context oauth2Context = config.getEntraOAuth2Context();
                if (oauth2Context == null) {
                    throw new IllegalArgumentException(
                        "Microsoft Entra authentication requires an OAuth 2.0 credential component"
                    );
                }
                try {
                    OAuth2Token oauthToken = oauth2Context.getOAuth2Token(false);
                    String accessToken = oauthToken.getAccessToken();
                    this.username = extractOidFromToken(accessToken);
                    this.password = accessToken;
                } catch (IOException e) {
                    throw new RuntimeException("Failed to obtain Microsoft Entra token", e);
                }
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

    /**
     * Extracts the 'oid' (Object ID) claim from an Azure AD JWT access token.
     * Azure Cache for Redis requires the OID as the AUTH username.
     */
    private static String extractOidFromToken(String token) {
        String[] parts = token.split("\\.");
        String base64 = parts[1];

        switch (base64.length() % 4) {
            case 2: base64 += "=="; break;
            case 3: base64 += "="; break;
            default: break;
        }

        byte[] jsonBytes = Base64.getDecoder().decode(base64);
        String json = new String(jsonBytes, StandardCharsets.UTF_8);
        JsonObject jwt = JsonParser.parseString(json).getAsJsonObject();
        return jwt.get("oid").getAsString();
    }

    @Override
    public boolean isValid() {
        return true;
    }
}