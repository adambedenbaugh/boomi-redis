package com.boomi.connector.redis.connection;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.XClaimParams;
import redis.clients.jedis.params.XPendingParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamPendingEntry;

import java.util.*;
import java.util.logging.Logger;

/**
 * Redis client implementation for standalone (single-node) Redis instances without connection pooling.
 * Uses a direct Jedis connection for all operations.
 */
public class StandaloneRedisConnection extends BaseRedisConnection {

    private static final Logger logger = Logger.getLogger(StandaloneRedisConnection.class.getName());
    private Jedis jedis;

    public StandaloneRedisConnection(RedisConnectionConfig config) {
        this(config, new DefaultJedisClientFactory());
    }

    StandaloneRedisConnection(RedisConnectionConfig config, JedisClientFactory clientFactory) {
        super(config, clientFactory);
        initializeConnection();
    }

    private void initializeConnection() {
        HostAndPort node = new HostAndPort(config.getHost(), config.getPort());
        jedis = clientFactory.createClient(node, buildClientConfig());
        logger.fine("Initialized standalone Redis connection to " + node);
    }
    
    @Override
    public String get(String key) {
        return jedis.get(key);
    }
    
    @Override
    public void set(String key, String value, Long ttl) {
        if (ttl != null && ttl != -1) {
            logger.fine("Setting key with TTL: " + key + " TTL: " + ttl);
            jedis.psetex(key, ttl, value);
        } else {
            logger.fine("Setting key without TTL: " + key);
            jedis.set(key, value);
        }
    }
    
    @Override
    public void del(String key) {
        jedis.del(key);
    }
    
    @Override
    public void delAll(String pattern) {
        String scanPattern = prepareScanPattern(pattern);
        ScanParams scanParams = new ScanParams().count(100).match(scanPattern);
        String cursor = ScanParams.SCAN_POINTER_START;
        
        do {
            ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
            List<String> keys = scanResult.getResult();
            
            if (!keys.isEmpty()) {
                // Pipelined single-key DELs (never a multi-key DEL) so the Enterprise Clustered
                // proxy never receives a cross-slot multi-key command.
                Pipeline pipeline = jedis.pipelined();
                for (String key : keys) {
                    pipeline.del(key);
                }
                pipeline.sync();
            }

            cursor = scanResult.getCursor();
        } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
    }

    @Override
    public Map<String, String> getAll(String pattern) {
        Map<String, String> result = new HashMap<>();
        String scanPattern = prepareScanPattern(pattern);
        ScanParams scanParams = new ScanParams().match(scanPattern).count(1000);
        String cursor = ScanParams.SCAN_POINTER_START;
        List<String> allKeys = new ArrayList<>();

        // Collect all matching keys using SCAN
        do {
            ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
            List<String> foundKeys = scanResult.getResult();
            allKeys.addAll(foundKeys);
            cursor = scanResult.getCursor();
        } while (!cursor.equals(ScanParams.SCAN_POINTER_START));

        // Get all values
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
    }
    
    @Override
    public boolean isValid() {
        try {
            jedis.ping();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void close() {
        if (jedis != null) {
            try {
                jedis.close();
            } catch (Exception e) {
                logger.warning("Error closing Jedis connection: " + e.getMessage());
            }
        }
        jedis = null;
    }

    @Override
    public void rpush(String key, String value) {
        jedis.rpush(key, value);
    }

    @Override
    public String blpop(String key, int timeoutSeconds) {
        List<String> result = jedis.blpop(timeoutSeconds, key);
        return result == null ? null : result.get(1);
    }

    @Override
    public void xgroupCreate(String key, String groupName) {
        try {
            jedis.xgroupCreate(key, groupName, new StreamEntryID(0), true);
        } catch (JedisDataException e) {
            if (e.getMessage() == null || !e.getMessage().contains("BUSYGROUP")) {
                throw e;
            }
        }
    }

    @Override
    public void xadd(String key, String value) {
        jedis.xadd(key, StreamEntryID.NEW_ENTRY, Collections.singletonMap("value", value));
    }

    @Override
    public List<RedisStreamMessage> xreadGroup(String key, String groupName, String consumerName, int count,
            int blockMillis) {
        XReadGroupParams params = new XReadGroupParams().count(count).block(blockMillis);
        Map<String, StreamEntryID> streams = Collections.singletonMap(key, StreamEntryID.UNRECEIVED_ENTRY);
        return toStreamMessages(jedis.xreadGroup(groupName, consumerName, params, streams));
    }

    @Override
    public void xack(String key, String groupName, String messageId) {
        jedis.xack(key, groupName, new StreamEntryID(messageId));
    }

    @Override
    public List<RedisStreamPendingMessage> xpendingIdle(String key, String groupName, long idleMillis, int count) {
        XPendingParams params = XPendingParams.xPendingParams().idle(idleMillis).count(count);
        List<RedisStreamPendingMessage> result = new ArrayList<>();
        for (StreamPendingEntry entry : jedis.xpending(key, groupName, params)) {
            result.add(new RedisStreamPendingMessage(entry.getID().toString(), entry.getDeliveredTimes()));
        }
        return result;
    }

    @Override
    public RedisStreamMessage xclaim(String key, String groupName, String consumerName, String messageId) {
        List<StreamEntry> claimed = jedis.xclaim(key, groupName, consumerName, 0L, XClaimParams.xClaimParams(),
                new StreamEntryID(messageId));
        if (claimed.isEmpty() || claimed.get(0) == null) {
            return null;
        }
        StreamEntry entry = claimed.get(0);
        return new RedisStreamMessage(entry.getID().toString(), entry.getFields().get("value"));
    }
}