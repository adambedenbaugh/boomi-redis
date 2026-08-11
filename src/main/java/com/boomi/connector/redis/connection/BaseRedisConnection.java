package com.boomi.connector.redis.connection;

import com.boomi.connector.redis.authentication.AuthenticationType;
import com.boomi.connector.redis.authentication.BoomiRedisCredentialsProvider;
import com.boomi.connector.redis.util.RedisUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.time.Duration;
import java.util.logging.Logger;

/**
 * Base class for Redis connection implementations.
 * Provides common functionality and utilities shared across different connection types.
 * Authentication is delegated to a {@link RedisCredentialsProvider} so pooled and clustered
 * connections always authenticate with current credentials (including rotating Entra tokens).
 */
public abstract class BaseRedisConnection implements RedisConnectionInterface {

    private static final Logger logger = Logger.getLogger(BaseRedisConnection.class.getName());
    protected final RedisConnectionConfig config;
    protected final AuthenticationType authenticationType;
    protected final JedisClientFactory clientFactory;
    protected final RedisCredentialsProvider credentialsProvider;

    protected BaseRedisConnection(RedisConnectionConfig config, JedisClientFactory clientFactory) {
        this.config = config;
        this.clientFactory = clientFactory;
        this.authenticationType = config.getAuthenticationType();
        this.credentialsProvider = new BoomiRedisCredentialsProvider(
                authenticationType, config.getUsername(), config.getPassword(), config.getEntraOAuth2Context());
    }

    /** Builds a client config; attaches the credentials provider only when auth is required. */
    protected JedisClientConfig buildClientConfig() {
        DefaultJedisClientConfig.Builder builder = DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(config.getConnectionTimeout())
                .socketTimeoutMillis(config.getSocketTimeout())
                .ssl(config.isSSLEnabled());
        if (authenticationType != AuthenticationType.NONE) {
            builder.credentialsProvider(credentialsProvider);
        }
        return builder.build();
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
        int maxTotal = config.getClusterMaxTotal();
        poolConfig.setMaxTotal(maxTotal);
        poolConfig.setMinIdle(Math.min(config.getMinPoolSize(), maxTotal));
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleDuration(Duration.ofSeconds(config.getMaxIdleTime()));
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30));
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);

        logger.info("Created Jedis Cluster Pool with size: " + maxTotal);
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

        // If no wildcards present, this is a literal prefix: escape any glob metacharacters it
        // contains (e.g. a key prefix like "cache[1]:") before appending * to find keys with this
        // prefix, so the pattern only ever matches more broadly on purpose, never by accident.
        if (!pattern.contains("*") && !pattern.contains("?")) {
            return RedisUtils.escapeGlobMetacharacters(pattern) + "*";
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

    @Override
    public boolean isValid() {
        return true;
    }
}
