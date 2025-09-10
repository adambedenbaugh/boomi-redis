package com.boomi.connector.redis;

import java.util.*;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.redis.authentication.AuthenticationType;
import com.boomi.connector.redis.authentication.MicrosoftEntraClientSecretCredential;
import com.boomi.connector.util.BaseConnection;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Connection;

public class RedisConnection extends BaseConnection<BrowseContext> {

    private static final int S_TIMEOUT = 2000;
    private static final int S_ATTEMPS = 3;

    private Jedis jedis;
    private JedisCluster jedisCluster;
    private JedisPool jedisPool;
    private AuthenticationType authenticationType;
    private boolean poolEnabled;
    private boolean isCluster;
    private PropertyMap propertiesMap;

    public RedisConnection(BrowseContext context) {
        super(context);
        propertiesMap = context.getConnectionProperties();
        init();
    }

    private void init() {
        String hosts = propertiesMap.getProperty("hosts");
        String user = propertiesMap.getProperty("user");
        String password = propertiesMap.getProperty("password");
        boolean useSSL = propertiesMap.getBooleanProperty("useSSL");
        poolEnabled = propertiesMap.getBooleanProperty("poolEnabled");
		int poolSize = 1;
        if(poolEnabled && propertiesMap.getLongProperty("poolSize") != null && propertiesMap.getLongProperty("poolSize") > 1) {
			poolSize = propertiesMap.getLongProperty("poolSize").intValue();
		}
        authenticationType = AuthenticationType.fromValue(propertiesMap.getProperty("authenticationType"));

        // Handle Microsoft Entra authentication
        if (authenticationType == AuthenticationType.MICROSOFT_ENTRA_CLIENT_SECRET_CREDENTIAL) {
            String clientId = propertiesMap.getProperty("clientId");
            String clientSecret = propertiesMap.getProperty("clientSecret");
            String tenantId = propertiesMap.getProperty("tenantId");

            MicrosoftEntraClientSecretCredential credential = 
                new MicrosoftEntraClientSecretCredential(tenantId, clientId, clientSecret);

            user = credential.getUsername();
            password = credential.getToken();
        }

        if (hosts == null || hosts.isEmpty()) {
            throwException(new Exception("Host is empty"));
            return;
        }

        initializeRedisConnection(hosts, user, password, useSSL, poolSize);
    }

    private void initializeRedisConnection(String hosts, String user, String password, boolean useSSL, int poolSize) {
        if (hosts.contains(",")) {
            // Cluster mode
            isCluster = true;
            initializeCluster(hosts, user, password, useSSL, poolSize);
        } else {
            // Single node
            isCluster = false;
            initializeSingleNode(hosts, user, password, useSSL, poolSize);
        }
    }

    private void initializeCluster(String hosts, String user, String password, boolean useSSL, int poolSize) {
        
        String[] pairs = hosts.split(",");
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        for (String pair : pairs) {
            String[] hostPort = pair.split(":");
            jedisClusterNodes.add(new HostAndPort(hostPort[0], Integer.parseInt(hostPort[1])));
        }

        GenericObjectPoolConfig<Connection> poolConfig = createConnectionPoolConfig(poolSize);

        if (authenticationType.requiresCredentials()) {
            // Create DefaultJedisClientConfig with authentication
            DefaultJedisClientConfig.Builder configBuilder = DefaultJedisClientConfig.builder()
                .socketTimeoutMillis(S_TIMEOUT)
                .connectionTimeoutMillis(S_TIMEOUT)
                .ssl(useSSL);
            
            if (user != null && password != null) {
                configBuilder.user(user).password(password);
            } else if (password != null) {
                configBuilder.password(password);
            }
            
            jedisCluster = new JedisCluster(
                jedisClusterNodes,
                configBuilder.build(),
                S_ATTEMPS,
                java.time.Duration.ofMillis(S_TIMEOUT),
                poolConfig
            );
        } else {
            // Create DefaultJedisClientConfig without authentication
            DefaultJedisClientConfig config = DefaultJedisClientConfig.builder()
                .socketTimeoutMillis(S_TIMEOUT)
                .connectionTimeoutMillis(S_TIMEOUT)
                .ssl(useSSL)
                .build();
                
            jedisCluster = new JedisCluster(
                jedisClusterNodes,
                config,
                S_ATTEMPS,
                java.time.Duration.ofMillis(S_TIMEOUT),
                poolConfig
            );
        }
    }

    private void initializeSingleNode(String hosts, String user, String password, boolean useSSL, int poolSize) {
        
        String[] pair = hosts.split(":");
        if (pair.length != 2) {
            throwException(new Exception("Invalid Redis Host: " + hosts + ". Syntax is <host>:<port>"));
            return;
        }

        String host = pair[0];
        int port = Integer.parseInt(pair[1]);

        if (poolEnabled) {
            GenericObjectPoolConfig<Jedis> poolConfig = createPoolConfig(poolSize);
            
            if (password != null && !password.isEmpty()) {
                jedisPool = new JedisPool(poolConfig, host, port, S_TIMEOUT, user, password, useSSL);
            } else {
                jedisPool = new JedisPool(poolConfig, host, port, S_TIMEOUT, useSSL);
            }
        } else {
			jedis = new Jedis(host, port, S_TIMEOUT, useSSL);
			if (password != null && !password.isEmpty()) {
				jedis.auth(user, password);
			} 
        }
    }

    // TODO remove the hard coded configs 
    private GenericObjectPoolConfig<Connection> createConnectionPoolConfig(int poolSize) {
        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxWait(java.time.Duration.ofMillis(S_TIMEOUT));
        poolConfig.setMaxTotal(poolSize);
        poolConfig.setMinIdle(1);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleDuration(java.time.Duration.ofSeconds(60));
        poolConfig.setTimeBetweenEvictionRuns(java.time.Duration.ofSeconds(30));
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        return poolConfig;
    }
    
    // TODO remove the hard coded configs 
    private GenericObjectPoolConfig<Jedis> createPoolConfig(int poolSize) {
        GenericObjectPoolConfig<Jedis> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxWait(java.time.Duration.ofMillis(S_TIMEOUT));
        poolConfig.setMaxTotal(poolSize);
        poolConfig.setMinIdle(1);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleDuration(java.time.Duration.ofSeconds(60));
        poolConfig.setTimeBetweenEvictionRuns(java.time.Duration.ofSeconds(30));
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        return poolConfig;
    }

    public boolean isValid() {
        try {
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private Jedis getJedis() {
        if (poolEnabled && jedisPool != null) {
            return jedisPool.getResource();
        } else {
            return jedis;
        }
    }

    private void releaseJedis(Jedis jedis) {
        if (jedis != null) {
            try {
                jedis.close();
            } catch (Exception e) {
                throw new RuntimeException("Error closing Jedis connection: " + e.getMessage(), e);
            }
        }
    }

    public Map<String, String> getAll(String pattern, Long ttl) {
        Map<String, String> result = new HashMap<>();
        List<String> keys;
        ScanResult<String> scanResult;
        Jedis jedis = null;

        try {
            ScanParams scanParams = new ScanParams().count(100).match(pattern);
            String cur = ScanParams.SCAN_POINTER_START;
            do {
                if (isCluster) {
                    scanResult = jedisCluster.scan(cur, scanParams);
                    keys = scanResult.getResult();
                    for (String key : keys) {
                        result.put(key, jedisCluster.get(key));
                    }
                } else {
                    jedis = getJedis();
                    scanResult = jedis.scan(cur, scanParams);
                    keys = scanResult.getResult();
                    // Get values one by one to avoid deprecated pipelined() method
                    for (String key : keys) {
                        result.put(key, jedis.get(key));
                    }
                }
                cur = scanResult.getCursor();
            } while (!cur.equals(ScanParams.SCAN_POINTER_START));
        } finally {
            if (jedis != null) {
                releaseJedis(jedis);
            }
        }
        return result;
    }

    public String getValue(String key, Long ttl) {
        String result;
        if (isCluster) {
            result = jedisCluster.get(key);
            if (ttl != null && ttl != -1) {
                jedisCluster.pexpire(key, ttl);
            }
        } else {
            Jedis jedis = getJedis();
            try {
                result = jedis.get(key);
                if (ttl != null && ttl != -1) {
                    jedis.pexpire(key, ttl);
                }
            } finally {
                releaseJedis(jedis);
            }
        }
        return result;
    }

    public void set(String key, String value, Long ttl) {
        if (isCluster) {
            if (ttl != null && ttl != -1) {
                jedisCluster.psetex(key, ttl, value);
            } else {
                jedisCluster.set(key, value);
            }
        } else {
            Jedis jedis = getJedis();
            try {
                if (ttl != null && ttl != -1) {
                    jedis.psetex(key, ttl, value);
                } else {
                    jedis.set(key, value);
                }
            } finally {
                releaseJedis(jedis);
            }
        }
    }

    public void del(String key) {
        if (isCluster) {
            jedisCluster.del(key);
        } else {
            Jedis jedis = getJedis();
            try {
                jedis.del(key);
            } finally {
                releaseJedis(jedis);
            }
        }
    }

    public void delAll(String pattern) {
        Jedis jedis = null;
        long numDel;
        try {
            ScanParams scanParams = new ScanParams().count(100).match(pattern);
            String cur = ScanParams.SCAN_POINTER_START;
            ScanResult<String> scanResult;
            do {
                if (isCluster) {
                    scanResult = jedisCluster.scan(cur, scanParams);
                } else {
                    jedis = getJedis();
                    scanResult = jedis.scan(cur, scanParams);
                }
                String[] arrKeys = scanResult.getResult().toArray(new String[0]);
                if (arrKeys.length > 0) {
                    if (isCluster) {
                        numDel = jedisCluster.del(arrKeys);
                    } else {
                        numDel = jedis.del(arrKeys);
                    }
                    // if (numDel != arrKeys.length) {
                    //     logger.warning("Could not delete " + (arrKeys.length - numDel) + " entries from Redis");
                    // }
                } //else {
                //     logger.fine("Nothing to delete");
                // }
                cur = scanResult.getCursor();
            } while (!cur.equals(ScanParams.SCAN_POINTER_START));
        } finally {
            if (jedis != null) {
                releaseJedis(jedis);
            }
        }
    }

    private void throwException(Throwable exception) {
        Utils.<RuntimeException>throwException(exception, null);
    }

    // Utility class for exception handling
    private static class Utils {
        @SuppressWarnings("unchecked")
        private static <T extends Throwable> void throwException(Throwable exception, Object dummy) throws T {
            throw (T) exception;
        }
    }
}