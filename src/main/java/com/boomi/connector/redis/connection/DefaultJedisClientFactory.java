package com.boomi.connector.redis.connection;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.time.Duration;
import java.util.Set;

/** Production factory calling the real Jedis constructors. */
public class DefaultJedisClientFactory implements JedisClientFactory {

    @Override
    public JedisPool createPool(GenericObjectPoolConfig<Jedis> poolConfig, HostAndPort node,
                                JedisClientConfig clientConfig) {
        return new JedisPool(poolConfig, node, clientConfig);
    }

    @Override
    public Jedis createClient(HostAndPort node, JedisClientConfig clientConfig) {
        return new Jedis(node, clientConfig);
    }

    @Override
    public JedisCluster createCluster(Set<HostAndPort> nodes, JedisClientConfig clientConfig, int maxAttempts,
                                      Duration maxTotalRetriesDuration, GenericObjectPoolConfig<Connection> poolConfig) {
        return new JedisCluster(nodes, clientConfig, maxAttempts, maxTotalRetriesDuration, poolConfig);
    }

    @Override
    public Jedis createClientFromConnection(Connection connection) {
        return new Jedis(connection);
    }
}
