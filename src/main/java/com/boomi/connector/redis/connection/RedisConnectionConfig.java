package com.boomi.connector.redis.connection;

import com.boomi.connector.api.BrowseContext;
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
    
    private final PropertyMap propertiesMap;
    private final String hosts;
    private final boolean useSSL;
    private final boolean poolEnabled;
    private final int connectionTimeout;
    private final int socketTimeout;
    private final AuthenticationType authenticationType;
    
    // Authentication properties
    private final String username;
    private final String password;
    private final String clientId;
    private final String clientSecret;
    private final String tenantId;
    
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
        
        this.useSSL = propertiesMap.getBooleanProperty("useSSL");
        this.poolEnabled = propertiesMap.getBooleanProperty("poolEnabled");
        
        // Parse timeout configurations - convert seconds to milliseconds
        this.connectionTimeout = propertiesMap.getLongProperty("connectionTimeout", 30L).intValue() * 1000;
        this.socketTimeout = propertiesMap.getLongProperty("socketTimeout", 30L).intValue() * 1000;
        
        // Parse authentication
        this.authenticationType = AuthenticationType.fromValue(propertiesMap.getProperty("authenticationType"));
        this.username = propertiesMap.getProperty("user");
        this.password = propertiesMap.getProperty("password");
        this.clientId = propertiesMap.getProperty("clientId");
        this.clientSecret = propertiesMap.getProperty("clientSecret");
        this.tenantId = propertiesMap.getProperty("tenantId");
        
        // Parse pool configuration
        this.poolSize = poolEnabled ? 1 : 0; // Default pool size
        this.minPoolSize = propertiesMap.getLongProperty("minPoolSize", 1L).intValue();
        this.maxIdleTime = propertiesMap.getLongProperty("maxIdleTime", 60L).intValue();
        this.maxWaitTime = propertiesMap.getLongProperty("maxWaitTime", 60L).intValue();
    }
    
    /**
     * Determines if this is a cluster configuration based on multiple hosts.
     */
    public boolean isCluster() {
        return hosts.contains(",");
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
        if (isCluster()) {
            throw new IllegalStateException("Cannot get single host from cluster configuration");
        }
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
        if (isCluster()) {
            throw new IllegalStateException("Cannot get single port from cluster configuration");
        }
        String[] parts = hosts.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid Redis Host: " + hosts + ". Syntax is <host>:<port>");
        }
        return Integer.parseInt(parts[1]);
    }
    
    /**
     * Parses cluster nodes from the hosts configuration.
     */
    public Set<HostAndPort> getClusterNodes() {
        if (!isCluster()) {
            throw new IllegalStateException("Cannot get cluster nodes from single-node configuration");
        }
        
        String[] pairs = hosts.split(",");
        Set<HostAndPort> nodes = new HashSet<>();
        
        for (String pair : pairs) {
            String[] hostPort = pair.trim().split(":");
            if (hostPort.length != 2) {
                throw new IllegalArgumentException("Invalid cluster node: " + pair + ". Syntax is <host>:<port>");
            }
            nodes.add(new HostAndPort(hostPort[0], Integer.parseInt(hostPort[1])));
        }
        
        return nodes;
    }
    
    // Getters
    public String getHosts() { return hosts; }
    public boolean isSSLEnabled() { return useSSL; }
    public int getConnectionTimeout() { return connectionTimeout; }
    public int getSocketTimeout() { return socketTimeout; }
    public AuthenticationType getAuthenticationType() { return authenticationType; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public String getClientId() { return clientId; }
    public String getClientSecret() { return clientSecret; }
    public String getTenantId() { return tenantId; }
    public int getPoolSize() { return poolSize; }
    public int getMinPoolSize() { return minPoolSize; }
    public int getMaxIdleTime() { return maxIdleTime; }
    public int getMaxWaitTime() { return maxWaitTime; }
}