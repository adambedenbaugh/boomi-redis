package com.boomi.connector.redis.connection;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Test;
import redis.clients.jedis.*;

import static org.junit.Assert.assertNotNull;

public class DefaultJedisClientFactoryTest {

    // Jedis/JedisPool constructors are lazy (no socket opened until first use),
    // so we can assert construction without a live Redis.
    @Test
    public void createsClientAndPoolWithoutConnecting() {
        DefaultJedisClientFactory factory = new DefaultJedisClientFactory();
        HostAndPort node = new HostAndPort("localhost", 6379);
        JedisClientConfig cfg = DefaultJedisClientConfig.builder().ssl(false).build();

        Jedis client = factory.createClient(node, cfg);
        assertNotNull(client);
        client.close();

        JedisPool pool = factory.createPool(new GenericObjectPoolConfig<Jedis>(), node, cfg);
        assertNotNull(pool);
        pool.close();
    }
}
