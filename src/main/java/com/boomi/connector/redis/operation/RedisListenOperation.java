package com.boomi.connector.redis.operation;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.listen.Listener;
import com.boomi.connector.api.listen.ListenerExecutionResult;
import com.boomi.connector.api.listen.SingletonListenOperation;
import com.boomi.connector.api.listen.SubmitOptions;
import com.boomi.connector.api.listen.options.WaitMode;
import com.boomi.connector.util.PayloadUtil;
import com.boomi.connector.redis.RedisConnection;
import com.boomi.connector.redis.connection.RedisStreamMessage;
import com.boomi.connector.redis.pool.RedisListenManager;

import com.google.gson.JsonObject;

public class RedisListenOperation implements SingletonListenOperation<RedisListenManager> {

    private static final Logger logger = Logger.getLogger(RedisListenOperation.class.getName());
    private static final String STREAM = "Stream";

    /**
     * Fallback bound (ms) for {@code future.get()} in ack-on-completion mode when the connection's
     * "Idle Reclaim Timeout" operation property is unavailable. See {@link #submitStreamMessage}.
     */
    private static final long DEFAULT_ACK_WAIT_TIMEOUT_MS = 60_000L;

    private final OperationContext context;
    private final String queueType;
    private final String queueKey;
    private final boolean singleton;
    private final int pollBlockSeconds;
    private final String consumerGroupName;
    private final String consumerName;
    private final boolean ackOnCompletion;
    private final long ackWaitTimeoutMs;

    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private volatile Thread pollThread;
    private volatile Listener listener;
    private volatile RedisConnection redisConnection;

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

        if (STREAM.equals(queueType)) {
            this.consumerGroupName = context.getConnectionProperties().getProperty("consumerGroupName");
            if (consumerGroupName == null || consumerGroupName.isEmpty()) {
                throw new ConnectorException("Queue Type \"Stream\" requires a \"Consumer Group Name\" "
                        + "set on the connection.");
            }
            this.consumerName = deriveConsumerName();
            this.ackOnCompletion = "OnCompletion".equals(
                    context.getOperationProperties().getProperty("ack_mode", "OnCompletion"));
            this.ackWaitTimeoutMs = longPropertyOrDefault(context.getOperationProperties(),
                    "idle_reclaim_timeout_ms", DEFAULT_ACK_WAIT_TIMEOUT_MS);
        } else {
            this.consumerGroupName = null;
            this.consumerName = null;
            this.ackOnCompletion = false;
            this.ackWaitTimeoutMs = 0L;
        }
    }

    /**
     * {@link PropertyMap#getLongProperty(String, Long)} is documented to fall back to the supplied
     * default, but that fallback is real-implementation behavior -- a bare mock in a unit test
     * returns {@code null} for any unstubbed key regardless of the default argument. Guard the
     * unboxing here so a missing/unstubbed property can never raw-NPE the constructor.
     */
    private static long longPropertyOrDefault(PropertyMap props, String key, long defaultValue) {
        Long value = props.getLongProperty(key, defaultValue);
        return value != null ? value : defaultValue;
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
        if (STREAM.equals(queueType)) {
            redisConnection.xgroupCreate(queueKey, consumerGroupName);
        }
        this.stopping.set(false);
        this.pollThread = new Thread(this::runPollLoop, "Redis Listen: " + queueKey);
        this.pollThread.setDaemon(true);
        this.pollThread.start();
    }

    @Override
    public void stop() {
        stopping.set(true);
        if (pollThread != null) {
            // interrupt() is harmless and helps when the poll thread happens to be sleeping in
            // backoff() rather than blocked inside a Redis call, but it has no effect on a
            // blocking Jedis socket read (blpop() reads from a plain java.net.Socket, not an
            // interruptible NIO channel). The join timeout below is what actually guarantees
            // correctness: it must always outlast the poll thread's current in-flight blocking
            // call (bounded by pollBlockSeconds) so join() returns because the thread genuinely
            // terminated -- not because it timed out while still reading from the connection we
            // are about to close.
            pollThread.interrupt();
            long joinTimeoutMs = Math.max(10_000L, (pollBlockSeconds * 1000L) + 5_000L);
            try {
                pollThread.join(joinTimeoutMs);
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
        if (STREAM.equals(queueType)) {
            pollStream();
        } else {
            pollList();
        }
    }

    private void pollList() {
        String value = redisConnection.blpop(queueKey, pollBlockSeconds);
        if (value != null) {
            listener.submit(PayloadUtil.toPayload(toJson(queueKey, value)));
        }
    }

    private void pollStream() {
        List<RedisStreamMessage> messages = redisConnection.xreadGroup(
                queueKey, consumerGroupName, consumerName, 1, pollBlockSeconds * 1000);
        for (RedisStreamMessage message : messages) {
            submitStreamMessage(message);
        }
    }

    /**
     * Submits one Stream message to Boomi and, in "on submission" mode, ACKs immediately; in
     * "on process completion" mode, waits for the process execution and ACKs only on success.
     *
     * <p><b>On the {@code future.get()} bound below:</b> the SDK's {@link Listener#submit(
     * com.boomi.connector.api.Payload, SubmitOptions)}/{@link SubmitOptions}/
     * {@link ListenerExecutionResult} contract offers no built-in bound on how long a submitted
     * process execution may run -- confirmed by inspecting connector-sdk-api 2.30.1 sources
     * (Listener, SubmitOptions, WaitMode carry no timeout concept). Calling the no-arg
     * {@code future.get()} would therefore let one hung/slow process execution block this poll
     * thread indefinitely -- the same class of problem as the {@code stop()} join-timeout bug fixed
     * immediately before this task, just on the Boomi-execution side instead of the Redis side. We
     * use the bounded {@code future.get(timeout, unit)} overload instead, bounded by the existing
     * "Idle Reclaim Timeout" operation property (falls back to {@link #DEFAULT_ACK_WAIT_TIMEOUT_MS}):
     * waiting past that point buys nothing, since the still-unacked message becomes eligible for
     * Task 8's reclaim sweep at that same threshold regardless.
     *
     * <p><b>Known limitation:</b> this bounds the wait, but does not change {@code stop()}'s
     * join-timeout formula, which was intentionally left untouched for this task (it was just fixed
     * for the unrelated Redis-blocking-call bug and re-touching it here was out of scope). That
     * formula sizes its budget only from {@code pollBlockSeconds}, so if {@code ackWaitTimeoutMs}
     * exceeds that budget, {@code stop()} can still return (its {@code join()} call times out) while
     * this thread is inside this bounded wait for an in-flight submission. That is strictly better
     * than the unbounded case (the thread is now guaranteed to finish within {@code ackWaitTimeoutMs}
     * and release the connection reference on its own), but a caller invoking {@code stop()} is not
     * guaranteed the poll thread has already terminated in that window. Recommended follow-up: have
     * {@code stop()} add {@code ackWaitTimeoutMs} to its join budget when Stream mode + ack-on-
     * completion is active.
     */
    void submitStreamMessage(RedisStreamMessage message) {
        String json = toJson(message.getId(), message.getValue());
        if (ackOnCompletion) {
            SubmitOptions options = new SubmitOptions().withWaitMode(WaitMode.PROCESS_COMPLETION);
            Future<ListenerExecutionResult> future = listener.submit(PayloadUtil.toPayload(json), options);
            try {
                ListenerExecutionResult result = future.get(ackWaitTimeoutMs, TimeUnit.MILLISECONDS);
                if (result.isSuccess()) {
                    redisConnection.xack(queueKey, consumerGroupName, message.getId());
                } else {
                    logger.warning("Process execution failed for stream message " + message.getId()
                            + " on " + queueKey + "; leaving unacknowledged for reclaim.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                logger.log(Level.SEVERE, "Error waiting for process completion for stream message "
                        + message.getId(), e);
            } catch (TimeoutException e) {
                logger.warning("Timed out after " + ackWaitTimeoutMs + "ms waiting for process "
                        + "completion for stream message " + message.getId() + " on " + queueKey
                        + "; leaving unacknowledged for reclaim.");
            }
        } else {
            listener.submit(PayloadUtil.toPayload(json));
            redisConnection.xack(queueKey, consumerGroupName, message.getId());
        }
    }

    private static String deriveConsumerName() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            host = "unknown-host";
        }
        return host + "-" + UUID.randomUUID();
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
