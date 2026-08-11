package com.boomi.connector.redis.pool;

import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.redis.authentication.AuthenticationType;
import com.boomi.connector.redis.connection.ClusteringPolicy;
import com.boomi.connector.redis.connection.RedisConnectionConfig;

import java.util.Objects;

/**
 * Value object holding the full identity of a Redis client configuration, used as the key for
 * {@link RedisClientPoolManager}'s shared-client map. Modeled on the official Boomi JMS V2
 * connector's {@code AdapterSettings}.
 *
 * <p>Two instances are equal when every connection field matches (value equality - the same keying
 * the JMS V2 connector uses). Consequences:
 * <ul>
 *   <li>Changing any connection field (credentials, timeouts, pool sizing, topology) produces a
 *       new key, so the next execution builds a client with the new values; the superseded client
 *       idles out via {@link RedisClientPoolManager}'s eviction.</li>
 *   <li>Two connection components with byte-identical settings share one client. This is benign
 *       (identical settings means the same endpoint and the same credentials) and matches the JMS
 *       V2 behavior. A best-effort {@code connectionId} participates in equality, but the Atom
 *       runtime supplies no component id (verified 2026-08-11 - see
 *       {@link RedisConnectionConfig#getConnectionId()}), so it is "" in production; per-component
 *       isolation activates automatically if a future runtime ever provides the id.</li>
 *   <li>The rotating Entra access token is deliberately NOT part of the identity - only the stable
 *       OAuth fields (client id, client secret, token URL) are - so token refresh keeps reusing
 *       the same pool.</li>
 * </ul>
 *
 * <p>Credentials participate in equality only for the authentication type that uses them
 * (mirroring AdapterSettings, which appends username/password only when authentication is on).
 * Secrets are held in memory for equality but are never exposed: {@link #toString()} redacts.
 */
public final class RedisClientSettings {

    private final String connectionId;
    private final String hosts;
    private final boolean useSSL;
    private final ClusteringPolicy clusteringPolicy;
    private final AuthenticationType authenticationType;
    private final String username;
    private final String password;
    private final String entraClientId;
    private final String entraClientSecret;
    private final String entraAccessTokenUrl;
    private final int connectionTimeoutMillis;
    private final int socketTimeoutMillis;
    private final boolean poolEnabled;
    private final int poolSize;
    private final int minPoolSize;
    private final int maxIdleTime;
    private final int maxWaitTime;

    public RedisClientSettings(RedisConnectionConfig config) {
        this.connectionId = config.getConnectionId();
        this.hosts = config.getHosts();
        this.useSSL = config.isSSLEnabled();
        this.clusteringPolicy = config.getClusteringPolicy();
        this.authenticationType = config.getAuthenticationType();

        boolean basic = authenticationType == AuthenticationType.BASIC;
        this.username = basic ? nullSafe(config.getUsername()) : "";
        this.password = basic ? nullSafe(config.getPassword()) : "";

        OAuth2Context oauth = authenticationType.isMicrosoftEntra() ? config.getEntraOAuth2Context() : null;
        this.entraClientId = oauth != null ? nullSafe(oauth.getClientId()) : "";
        this.entraClientSecret = oauth != null ? nullSafe(oauth.getClientSecret()) : "";
        this.entraAccessTokenUrl = oauth != null ? nullSafe(oauth.getAccessTokenUrl()) : "";

        this.connectionTimeoutMillis = config.getConnectionTimeout();
        this.socketTimeoutMillis = config.getSocketTimeout();
        this.poolEnabled = config.isPoolEnabled();
        this.poolSize = config.getPoolSize();
        this.minPoolSize = config.getMinPoolSize();
        this.maxIdleTime = config.getMaxIdleTime();
        this.maxWaitTime = config.getMaxWaitTime();
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }

    public boolean isPoolEnabled() {
        return poolEnabled;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RedisClientSettings)) {
            return false;
        }
        RedisClientSettings other = (RedisClientSettings) o;
        return useSSL == other.useSSL
                && connectionTimeoutMillis == other.connectionTimeoutMillis
                && socketTimeoutMillis == other.socketTimeoutMillis
                && poolEnabled == other.poolEnabled
                && poolSize == other.poolSize
                && minPoolSize == other.minPoolSize
                && maxIdleTime == other.maxIdleTime
                && maxWaitTime == other.maxWaitTime
                && clusteringPolicy == other.clusteringPolicy
                && authenticationType == other.authenticationType
                && Objects.equals(connectionId, other.connectionId)
                && Objects.equals(hosts, other.hosts)
                && Objects.equals(username, other.username)
                && Objects.equals(password, other.password)
                && Objects.equals(entraClientId, other.entraClientId)
                && Objects.equals(entraClientSecret, other.entraClientSecret)
                && Objects.equals(entraAccessTokenUrl, other.entraAccessTokenUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectionId, hosts, useSSL, clusteringPolicy, authenticationType,
                username, password, entraClientId, entraClientSecret, entraAccessTokenUrl,
                connectionTimeoutMillis, socketTimeoutMillis, poolEnabled, poolSize, minPoolSize,
                maxIdleTime, maxWaitTime);
    }

    /**
     * Redacting: identifies the client without exposing credentials. Safe to log.
     * {@code connectionId} is deliberately omitted - the Atom runtime never supplies it, so it
     * would render as a confusing {@code connectionId=''} in every pool log line. (It still
     * participates in {@link #equals}/{@link #hashCode}.)
     */
    @Override
    public String toString() {
        return "RedisClientSettings{hosts='" + hosts
                + "', clusteringPolicy=" + clusteringPolicy + ", authenticationType=" + authenticationType
                + ", poolEnabled=" + poolEnabled + "}";
    }
}
