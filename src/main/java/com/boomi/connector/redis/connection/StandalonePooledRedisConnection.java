package com.boomi.connector.redis.connection;

import com.boomi.connector.redis.pool.RedisClientPoolManager;
import com.boomi.connector.redis.pool.RedisClientSettings;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.*;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * Redis client implementation for standalone (single-node) Redis instances with connection pooling.
 * The shared {@link JedisPool} is owned by {@link RedisClientPoolManager}, keyed by the full
 * connection identity ({@link RedisClientSettings}: every connection field), reused across
 * executions, and closed by the manager's idle eviction - never by this class.
 */
public class StandalonePooledRedisConnection extends BaseRedisConnection {

    private static final Logger logger = Logger.getLogger(StandalonePooledRedisConnection.class.getName());

    private JedisPool jedisPool;
    private RedisClientSettings settings;

    public StandalonePooledRedisConnection(RedisConnectionConfig config) {
        this(config, new DefaultJedisClientFactory());
    }

    StandalonePooledRedisConnection(RedisConnectionConfig config, JedisClientFactory clientFactory) {
        super(config, clientFactory);
        this.settings = new RedisClientSettings(config);
        this.jedisPool = RedisClientPoolManager.acquire(settings, new Supplier<JedisPool>() {
            @Override
            public JedisPool get() {
                HostAndPort node = new HostAndPort(config.getHost(), config.getPort());
                logger.info("Creating new shared Redis connection pool to " + node
                        + " with pool size: " + config.getPoolSize());
                return clientFactory.createPool(createJedisPoolConfig(), node, buildClientConfig());
            }
        });
    }

    @Override
    public String get(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        } catch (Exception e) {
            logger.warning("Error getting key " + key + ": " + e.getMessage());
            throw e;
        }
    }

    @Override
    public void set(String key, String value, Long ttl) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (ttl != null && ttl != -1) {
                logger.fine("Setting key with TTL: " + key + " TTL: " + ttl);
                jedis.psetex(key, ttl, value);
            } else {
                logger.fine("Setting key without TTL: " + key);
                jedis.set(key, value);
            }
        } catch (Exception e) {
            logger.warning("Error setting key " + key + ": " + e.getMessage());
            throw e;
        }
    }

    @Override
    public void del(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(key);
        } catch (Exception e) {
            logger.warning("Error deleting key " + key + ": " + e.getMessage());
            throw e;
        }
    }

    @Override
    public void delAll(String pattern) {
        try (Jedis jedis = jedisPool.getResource()) {
            String scanPattern = prepareScanPattern(pattern);
            ScanParams scanParams = new ScanParams().count(100).match(scanPattern);
            String cursor = ScanParams.SCAN_POINTER_START;

            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                List<String> keys = scanResult.getResult();

                if (!keys.isEmpty()) {
                    // Pipelined single-key DELs; never a cross-slot multi-key DEL.
                    Pipeline pipeline = jedis.pipelined();
                    for (String key : keys) {
                        pipeline.del(key);
                    }
                    pipeline.sync();
                }

                cursor = scanResult.getCursor();
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        } catch (Exception e) {
            logger.warning("Error deleting keys with pattern " + pattern + ": " + e.getMessage());
            throw e;
        }
    }

    @Override
    public Map<String, String> getAll(String pattern) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> result = new HashMap<>();
            String scanPattern = prepareScanPattern(pattern);
            ScanParams scanParams = new ScanParams().match(scanPattern).count(1000);
            String cursor = ScanParams.SCAN_POINTER_START;
            List<String> allKeys = new ArrayList<>();

            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                List<String> foundKeys = scanResult.getResult();
                allKeys.addAll(foundKeys);
                cursor = scanResult.getCursor();
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));

            if (!allKeys.isEmpty()) {
                Pipeline pipeline = jedis.pipelined();
                List<Response<String>> responses = new ArrayList<>();

                for (String key : allKeys) {
                    responses.add(pipeline.get(key));
                }

                pipeline.sync();

                // Collect results
                for (int i = 0; i < allKeys.size(); i++) {
                    String key = allKeys.get(i);
                    String value = responses.get(i).get();
                    if (value != null) {
                        result.put(key, value);
                    }
                }
            }

            return result;
        } catch (Exception e) {
            logger.warning("Error getting all keys with pattern " + pattern + ": " + e.getMessage());
            throw e;
        }
    }

    @Override
    public boolean isValid() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.ping();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Releases this instance's reference to the shared pool via the manager. The pool itself
     * stays registered for reuse by the next execution; it is closed only by the manager's idle
     * eviction or {@link RedisClientPoolManager#closeAll()}.
     */
    @Override
    public void close() {
        if (jedisPool != null) {
            RedisClientPoolManager.release(settings, jedisPool);
            jedisPool = null;
            settings = null;
        }
    }
}
