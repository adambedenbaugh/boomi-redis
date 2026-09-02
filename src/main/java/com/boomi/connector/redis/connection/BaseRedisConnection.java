package com.boomi.connector.redis.connection;

import com.boomi.connector.redis.authentication.AuthenticationType;
import com.boomi.connector.redis.authentication.BoomiRedisCredentialsProvider;
import com.boomi.connector.redis.util.RedisUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.resps.StreamEntry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Base class for Redis connection implementations.
 * Provides common functionality and utilities shared across different connection types.
 * Authentication is delegated to a {@link RedisCredentialsProvider} so pooled and clustered
 * connections always authenticate with current credentials (including rotating Entra tokens).
 */
public abstract class BaseRedisConnection implements RedisConnectionInterface {

    private static final Logger logger = Logger.getLogger(BaseRedisConnection.class.getName());

    /**
     * Multiplier applied to the connection's configured socket timeout to derive a bounded
     * {@code blockingSocketTimeoutMillis} for Jedis's blocking commands (BLPOP, XREADGROUP with
     * BLOCK). See {@link #buildClientConfig()}.
     */
    private static final long BLOCKING_SOCKET_TIMEOUT_MULTIPLIER = 12L;

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

    /**
     * Builds a client config; attaches the credentials provider only when auth is required.
     *
     * <p>Also sets a bounded {@code blockingSocketTimeoutMillis}. Jedis 5.2's blocking commands
     * (BLPOP, XREADGROUP with BLOCK) call {@code setTimeoutInfinite()} internally, which uses this
     * specific config value as the actual socket timeout for the blocking call -- left unset, it
     * defaults to 0 (infinite). Without a finite bound here, a silent network failure (a firewall
     * drop or a cloud load-balancer idle-timeout kill -- neither sends an RST/FIN) can hang a
     * blocking call forever regardless of the caller's own timeout argument (e.g. Redis Listen's
     * "Poll Block" setting), which would falsify the "bounded by Poll Block" assumption the
     * Listen operation's {@code stop()}/join-timeout design rests on. The bound is derived from
     * this connection's own configured socket timeout (already operator-tunable -- e.g. Azure
     * Redis guidance in this connector's docs is to raise it to 30s) with a generous multiplier so
     * it stays comfortably larger than realistic blocking-command durations, plus the connection
     * timeout as an extra safety margin, while never being left at the infinite default.
     */
    protected JedisClientConfig buildClientConfig() {
        long blockingSocketTimeoutMillis =
                (config.getSocketTimeout() * BLOCKING_SOCKET_TIMEOUT_MULTIPLIER) + config.getConnectionTimeout();
        DefaultJedisClientConfig.Builder builder = DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(config.getConnectionTimeout())
                .socketTimeoutMillis(config.getSocketTimeout())
                .blockingSocketTimeoutMillis((int) blockingSocketTimeoutMillis)
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

        // INFO only for the shared (pooled) client, which is created rarely; the unpooled path
        // builds a private client on every execution and would flood the container log at INFO.
        if (config.isPoolEnabled()) {
            logger.info("Created Jedis Cluster Pool with size: " + maxTotal);
        } else {
            logger.fine("Created Jedis Cluster Pool with size: " + maxTotal);
        }
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

    @Override
    public boolean isValid() {
        return true;
    }

    /**
     * Flattens the raw {@code XREADGROUP}/multi-stream result shape (shared identically by
     * {@code Jedis} and {@code JedisCluster}, both via {@code UnifiedJedis}) into this connector's
     * plain {@link RedisStreamMessage} DTO. A null result (block timeout, no new entries) yields an
     * empty list.
     */
    protected static List<RedisStreamMessage> toStreamMessages(
            List<Map.Entry<String, List<StreamEntry>>> raw) {
        List<RedisStreamMessage> messages = new ArrayList<>();
        if (raw == null) {
            return messages;
        }
        for (Map.Entry<String, List<StreamEntry>> streamEntries : raw) {
            for (StreamEntry entry : streamEntries.getValue()) {
                messages.add(new RedisStreamMessage(entry.getID().toString(), entry.getFields().get("value")));
            }
        }
        return messages;
    }
}
