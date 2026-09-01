package com.boomi.connector.redis.operation;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.listen.Listener;
import com.boomi.connector.api.listen.SingletonListenOperation;
import com.boomi.connector.util.PayloadUtil;
import com.boomi.connector.redis.RedisConnection;
import com.boomi.connector.redis.pool.RedisListenManager;

import com.google.gson.JsonObject;

public class RedisListenOperation implements SingletonListenOperation<RedisListenManager> {

    private static final Logger logger = Logger.getLogger(RedisListenOperation.class.getName());
    private static final String STREAM = "Stream";

    private final OperationContext context;
    private final String queueType;
    private final String queueKey;
    private final boolean singleton;
    private final int pollBlockSeconds;

    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private Thread pollThread;
    private Listener listener;
    private RedisConnection redisConnection;

    public RedisListenOperation(OperationContext context) {
        this.context = context;
        this.queueType = context.getOperationProperties().getProperty("queue_type", "List");
        this.queueKey = context.getOperationProperties().getProperty("queue_key", "");
        this.singleton = context.getOperationProperties().getBooleanProperty("singleton_mode", Boolean.FALSE);
        long pollBlockMs = context.getOperationProperties().getLongProperty("poll_block_ms", 5000L);
        this.pollBlockSeconds = (int) Math.max(1L, pollBlockMs / 1000L);

        if (queueKey == null || queueKey.isEmpty()) {
            throw new ConnectorException("The Listen operation requires a non-empty \"Queue Key\".");
        }
    }

    @Override
    public boolean isSingleton() {
        return singleton;
    }

    @Override
    public void start(Listener listener, RedisListenManager manager) {
        this.listener = listener;
        this.redisConnection = new RedisConnection(context);
        this.redisConnection.init();
        this.stopping.set(false);
        this.pollThread = new Thread(this::runPollLoop, "Redis Listen: " + queueKey);
        this.pollThread.setDaemon(true);
        this.pollThread.start();
    }

    @Override
    public void stop() {
        stopping.set(true);
        if (pollThread != null) {
            pollThread.interrupt();
            try {
                pollThread.join(10_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            pollThread = null;
        }
        if (redisConnection != null) {
            redisConnection.close();
            redisConnection = null;
        }
    }

    private void runPollLoop() {
        while (!stopping.get() && !listener.isStopping()) {
            try {
                pollOnce();
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error while polling Redis queue " + queueKey, e);
                backoff();
            }
        }
    }

    private void pollOnce() {
        pollList();
    }

    private void pollList() {
        String value = redisConnection.blpop(queueKey, pollBlockSeconds);
        if (value != null) {
            listener.submit(PayloadUtil.toPayload(toJson(queueKey, value)));
        }
    }

    private void backoff() {
        try {
            Thread.sleep(5000L);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    static String toJson(String key, String value) {
        JsonObject obj = new JsonObject();
        obj.addProperty("key", key);
        obj.addProperty("value", value);
        return obj.toString();
    }
}
