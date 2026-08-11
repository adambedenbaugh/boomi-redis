package com.boomi.connector.redis.connection;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.redis.authentication.AuthenticationType;
import redis.clients.jedis.HostAndPort;

import java.util.HashSet;
import java.util.Set;

/**
 * Configuration class that encapsulates all Redis connection parameters.
 * Handles property parsing, validation, and provides typed access to configuration values.
 */
public class RedisConnectionConfig {

    /**
     * Cluster internal per-node pool size used when pooling is disabled. When pooling is off,
     * {@link ClusteredRedisConnection} builds a private, unshared {@code JedisCluster} used by a
     * single execution and torn down immediately after (mirroring the standalone "no pooling" path) -
     * so it never needs more than one live connection per node at a time, regardless of how many
     * shards the cluster has.
     */
    static final int UNPOOLED_CLUSTER_MAX_TOTAL = 1;

    private final PropertyMap propertiesMap;
    private final String hosts;
    private final String connectionId;
    private final boolean useSSL;
    private final boolean poolEnabled;
    private final int connectionTimeout;
    private final int socketTimeout;
    private final AuthenticationType authenticationType;
    private final ClusteringPolicy clusteringPolicy;

    // Authentication properties
    private final String username;
    private final String password;
    private final OAuth2Context entraOAuth2Context;
    
    // Pool configuration
    private final int poolSize;
    private final int minPoolSize;
    private final int maxIdleTime;
    private final int maxWaitTime;
    
    public RedisConnectionConfig(BrowseContext context) {
        this.propertiesMap = context.getConnectionProperties();
        
        // Parse connection properties
        this.hosts = propertiesMap.getProperty("hosts");
        if (hosts == null || hosts.isEmpty()) {
            throw new IllegalArgumentException("Host is empty");
        }

        // The Boomi runtime injects the connection component's id under the "id" property key.
        // It is part of the shared-client identity: two connection components with identical
        // values are still two distinct components and must get two distinct pools.
        String id = propertiesMap.getProperty("id");
        this.connectionId = (id == null) ? "" : id;

        this.useSSL = propertiesMap.getBooleanProperty("useSSL", Boolean.FALSE);
        this.poolEnabled = propertiesMap.getBooleanProperty("poolEnabled", Boolean.FALSE);
        
        // Parse timeout configurations - convert seconds to milliseconds
        this.connectionTimeout = propertiesMap.getLongProperty("connectionTimeout", 5L).intValue() * 1000;
        this.socketTimeout = propertiesMap.getLongProperty("socketTimeout", 5L).intValue() * 1000;
        
        // Parse authentication
        this.authenticationType = AuthenticationType.fromValue(propertiesMap.getProperty("authenticationType"));
        this.username = propertiesMap.getProperty("user");
        this.password = propertiesMap.getProperty("password");
        this.entraOAuth2Context = propertiesMap.getOAuth2Context("entraOAuth2");
        
        // Parse pool configuration
        this.poolSize = poolEnabled ? propertiesMap.getLongProperty("poolSize", 4L).intValue() : 0;
        this.minPoolSize = propertiesMap.getLongProperty("minPoolSize", 1L).intValue();
        this.maxIdleTime = propertiesMap.getLongProperty("maxIdleTime", 60L).intValue();
        this.maxWaitTime = propertiesMap.getLongProperty("maxWaitTime", 5L).intValue();

        this.clusteringPolicy = ClusteringPolicy.fromValue(propertiesMap.getProperty("clusteringPolicy"));
    }

    /** The declared Redis topology. */
    public ClusteringPolicy getClusteringPolicy() {
        return clusteringPolicy;
    }

    /** True when the target is a client-sharded OSS cluster (needs JedisCluster). */
    public boolean isOssCluster() {
        return clusteringPolicy.isOssCluster();
    }

    /**
     * Determines if connection pooling is enabled.
     */
    public boolean isPoolEnabled() {
        return poolEnabled;
    }
    
    /**
     * Gets the first host for single-node connections.
     */
    public String getHost() {
        String[] parts = hosts.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid Redis Host: " + hosts + ". Syntax is <host>:<port>");
        }
        return parts[0];
    }
    
    /**
     * Gets the port for single-node connections.
     */
    public int getPort() {
        String[] parts = hosts.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid Redis Host: " + hosts + ". Syntax is <host>:<port>");
        }
        return parsePort(parts[1], hosts);
    }

    /**
     * Parses cluster nodes from the hosts configuration.
     */
    public Set<HostAndPort> getClusterNodes() {
        String[] pairs = hosts.split(",");
        Set<HostAndPort> nodes = new HashSet<>();

        for (String pair : pairs) {
            String[] hostPort = pair.trim().split(":");
            if (hostPort.length != 2) {
                throw new IllegalArgumentException("Invalid cluster node: " + pair + ". Syntax is <host>:<port>");
            }
            nodes.add(new HostAndPort(hostPort[0], parsePort(hostPort[1], pair)));
        }

        return nodes;
    }

    /** Parses a port number, naming the offending value and its source in the error on failure. */
    private static int parsePort(String portValue, String source) {
        try {
            return Integer.parseInt(portValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port '" + portValue + "' in Hosts value '" + source
                    + "'. The port must be an integer.", e);
        }
    }
    
    /**
     * Stable identity for pool keying. Includes the stable credential material so that changing
     * credentials produces a different key — a new pool bound to the new credentials — while the
     * rotating Entra access token is deliberately excluded so token refresh never churns the key.
     *
     * <p>For Entra this keys on client id + a hash of the (stable) client secret + the access-token
     * URL. Consequently, editing the OAuth 2.0 component's client secret or token URL (e.g. fixing a
     * bad secret, or switching Azure Commercial -> Government) causes every new connection/execution
     * to build and use a pool for the new credentials instead of reusing a stale one. The client
     * secret is hashed so the key never contains secret material verbatim; Basic auth keys on
     * username + password hash for the same reason.
     */
    public String getAuthIdentity() {
        switch (authenticationType) {
            case BASIC:
                return "basic:" + username + ":" + java.util.Objects.hashCode(password);
            case MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL:
                if (entraOAuth2Context == null) {
                    return "entra:";
                }
                return "entra:" + entraOAuth2Context.getClientId()
                        + ":" + java.util.Objects.hashCode(entraOAuth2Context.getClientSecret())
                        + ":" + entraOAuth2Context.getAccessTokenUrl();
            default:
                return "none";
        }
    }

    // Getters
    public String getHosts() { return hosts; }

    /** The Boomi connection component id ("" when not supplied, e.g. in unit tests). */
    public String getConnectionId() { return connectionId; }
    public boolean isSSLEnabled() { return useSSL; }
    public int getConnectionTimeout() { return connectionTimeout; }
    public int getSocketTimeout() { return socketTimeout; }
    public AuthenticationType getAuthenticationType() { return authenticationType; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public OAuth2Context getEntraOAuth2Context() { return entraOAuth2Context; }
    public int getPoolSize() { return poolSize; }

    /**
     * Maximum connections for JedisCluster's internal per-node pool. JedisCluster is always
     * pooled, so this must be greater than zero even when "Enable Connection Pooling" is off. When
     * pooling is enabled, the shared cluster client persists across executions (see
     * {@link ClusteredRedisConnection}) and uses the configured Maximum Connections so concurrent
     * executions can each borrow a connection; when disabled, the cluster client is rebuilt fresh per
     * execution and never shared, so it only ever needs {@link #UNPOOLED_CLUSTER_MAX_TOTAL}.
     */
    public int getClusterMaxTotal() {
        return poolSize > 0 ? poolSize : UNPOOLED_CLUSTER_MAX_TOTAL;
    }

    public int getMinPoolSize() { return minPoolSize; }
    public int getMaxIdleTime() { return maxIdleTime; }
    public int getMaxWaitTime() { return maxWaitTime; }
}
