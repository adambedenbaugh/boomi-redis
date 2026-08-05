package com.boomi.connector.redis.connection;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.time.Duration;
import java.util.Set;

/** Seam over Jedis client construction so pool/connection lifecycle can be unit-tested. */
public interface JedisClientFactory {

    JedisPool createPool(GenericObjectPoolConfig<Jedis> poolConfig, HostAndPort node, JedisClientConfig clientConfig);

    Jedis createClient(HostAndPort node, JedisClientConfig clientConfig);

    JedisCluster createCluster(Set<HostAndPort> nodes, JedisClientConfig clientConfig, int maxAttempts,
                               Duration maxTotalRetriesDuration, GenericObjectPoolConfig<Connection> poolConfig);
}
