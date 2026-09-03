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
import java.util.concurrent.atomic.AtomicLong;
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

import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisListenOperation implements SingletonListenOperation<RedisListenManager> {

    private static final Logger logger = Logger.getLogger(RedisListenOperation.class.getName());
    private static final String STREAM = "Stream";

    /**
     * Fallback bound (ms) for {@code future.get()} in ack-on-completion mode when the connection's
     * "Idle Reclaim Timeout" operation property is unavailable. See {@link #submitStreamMessage}.
     */
    private static final long DEFAULT_ACK_WAIT_TIMEOUT_MS = 60_000L;

    /**
     * Ceiling (ms) for {@link #computeJoinTimeoutMs()}'s return value. See that method's javadoc:
     * the {@link #generation} counter -- not the join timeout -- is what actually prevents a
     * stale poll thread from resurrecting after a timed-out {@code stop()}, so the join timeout no
     * longer needs to scale unboundedly with operator-configured values like
     * {@code idle_reclaim_timeout_ms}.
     */
    private static final long MAX_JOIN_TIMEOUT_MS = 30_000L;

    /**
     * How often a SEVERE "Error while polling" entry may repeat while an outage continues. The
     * first failure in a run is always logged (full visibility into a new problem); Jedis never
     * repairs a broken {@code Connection} internally, so before {@link #reconnectQuietly()} existed
     * a dead connection meant the identical exception logged every {@link #backoff()} cycle (5s)
     * until a human manually stopped/started the listener -- observed in production as ~4800
     * identical stack traces over ~7 hours from a single stalled network read. Throttling repeats
     * to at most once per this window keeps that failure mode from being able to fill the container
     * log at scale, while {@link #shouldLogFailure} still logs a fresh entry once the outage clears
     * and recurs.
     */
    static final long FAILURE_LOG_THROTTLE_MS = TimeUnit.MINUTES.toMillis(5);

    private final OperationContext context;
    private final String queueType;
    private final String queueKey;
    private final boolean singleton;
    private final int pollBlockSeconds;
    private final String consumerGroupName;
    private final String consumerName;
    private final boolean ackOnCompletion;
    private final long ackWaitTimeoutMs;
    private final long idleReclaimTimeoutMs;
    private final long maxDeliveryAttempts;
    private final String deadLetterKey;

    private final AtomicBoolean stopping = new AtomicBoolean(false);

    /**
     * Incremented once per {@link #start()} call; the poll thread captures the resulting value at
     * startup and re-checks it on every loop iteration (see {@link #runPollLoop(long)}). This makes
     * it structurally impossible for a poll thread from a previous start()/stop() cycle to keep
     * looping after a new start() has occurred -- regardless of {@link #computeJoinTimeoutMs()}'s
     * timeout arithmetic. Without this, a poll thread still blocked past stop()'s join timeout
     * would see `stopping` reset to false by a subsequent start() and resume looping, potentially
     * running concurrently with the NEW poll thread against the NEW (non-thread-safe) Jedis
     * connection.
     */
    private final AtomicLong generation = new AtomicLong();
    private volatile Thread pollThread;
    private volatile Listener listener;
    private volatile RedisConnection redisConnection;
    private volatile long lastReclaimSweepAt = 0L;
    private volatile int consecutiveFailures = 0;
    private volatile long lastLoggedFailureAtMillis = 0L;

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
            this.consumerGroupName = context.getOperationProperties().getProperty("consumer_group_name", "");
            if (consumerGroupName == null || consumerGroupName.isEmpty()) {
                throw new ConnectorException("Queue Type \"Stream\" requires a \"Consumer Group Name\" "
                        + "set on this operation.");
            }
            this.consumerName = deriveConsumerName();
            this.ackOnCompletion = "OnCompletion".equals(
                    context.getOperationProperties().getProperty("ack_mode", "OnCompletion"));
            this.ackWaitTimeoutMs = longPropertyOrDefault(context.getOperationProperties(),
                    "idle_reclaim_timeout_ms", DEFAULT_ACK_WAIT_TIMEOUT_MS);
            this.idleReclaimTimeoutMs = longPropertyOrDefault(context.getOperationProperties(),
                    "idle_reclaim_timeout_ms", 60000L);
            this.maxDeliveryAttempts = longPropertyOrDefault(context.getOperationProperties(),
                    "max_delivery_attempts", 5L);
            String configuredDeadLetterKey = context.getOperationProperties().getProperty("dead_letter_key", "");
            this.deadLetterKey = (configuredDeadLetterKey == null || configuredDeadLetterKey.isEmpty())
                    ? queueKey + ":dead-letter" : configuredDeadLetterKey;
        } else {
            this.consumerGroupName = null;
            this.consumerName = null;
            this.ackOnCompletion = false;
            this.ackWaitTimeoutMs = 0L;
            this.idleReclaimTimeoutMs = 0L;
            this.maxDeliveryAttempts = 0L;
            this.deadLetterKey = null;
        }
    }

    String getDeadLetterKeyForTest() {
        return deadLetterKey;
    }

    long computeJoinTimeoutMsForTest() {
        return computeJoinTimeoutMs();
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
        // Build and initialize the connection into a LOCAL variable first, not the instance field.
        // If init() or xgroupCreate() throws (wrong Redis type at Queue Key, unreachable Redis,
        // etc.), the exception must still propagate out of start() (the SDK contract requires
        // start() to throw on failure) -- but the connection must never leak: on the pooled path a
        // leaked reference permanently pins the shared pool's active-reference count so the
        // evictor can never reclaim it, and on the unpooled path it leaks a live socket. Only
        // assigning `this.redisConnection` after setup has fully succeeded also means a
        // partially-failed start() leaves the instance's state clean for a subsequent start()
        // attempt, rather than pointing at a half-initialized connection.
        RedisConnection connection = new RedisConnection(context);
        try {
            connection.init();
            if (STREAM.equals(queueType)) {
                connection.xgroupCreate(queueKey, consumerGroupName);
            }
        } catch (RuntimeException e) {
            connection.close();
            throw e;
        }
        this.redisConnection = connection;
        final long myGeneration = generation.incrementAndGet();
        this.stopping.set(false);
        this.pollThread = new Thread(() -> runPollLoop(myGeneration), "Redis Listen: " + queueKey);
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
            // call (bounded by pollBlockSeconds) -- and, in Stream ack-on-completion mode, the
            // in-flight future.get(ackWaitTimeoutMs, ...) wait in submitStreamMessage() -- so
            // join() returns because the thread genuinely terminated -- not because it timed out
            // while the poll thread is still using the connection we are about to close.
            pollThread.interrupt();
            long joinTimeoutMs = computeJoinTimeoutMs();
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

    /**
     * Computes how long {@link #stop()} should wait on {@code pollThread.join(...)}.
     *
     * <p>The base budget covers the Redis-side blocking call ({@code blpop}/{@code xreadGroup}),
     * sized from {@code pollBlockSeconds} with a floor of 10s and a 5s safety margin. In Stream
     * mode with ack-on-completion, the poll thread can also be parked in
     * {@code submitStreamMessage()}'s bounded {@code future.get(ackWaitTimeoutMs, ...)} call --
     * a second, independent source of in-flight blocking that the base budget knows nothing
     * about. When that mode is active, the join timeout must cover whichever of the two the poll
     * thread might currently be inside, so it is the max of both components (each with its own
     * 5s safety margin).
     *
     * <p>The result is capped at {@link #MAX_JOIN_TIMEOUT_MS} regardless of how large
     * {@code pollBlockSeconds}/{@code ackWaitTimeoutMs} are configured. This is safe because
     * {@link #generation} (not this join timeout) is what actually prevents a stale poll thread
     * from resurrecting once {@code stop()} gives up waiting -- so an operator raising
     * {@code idle_reclaim_timeout_ms} to accommodate slow processes no longer silently makes every
     * listener stop (Atom shutdown/redeploy) take proportionally longer.
     */
    private long computeJoinTimeoutMs() {
        long redisBlockTimeoutMs = Math.max(10_000L, (pollBlockSeconds * 1000L) + 5_000L);
        long joinTimeoutMs = redisBlockTimeoutMs;
        if (STREAM.equals(queueType) && ackOnCompletion) {
            long ackWaitBudgetMs = ackWaitTimeoutMs + 5_000L;
            joinTimeoutMs = Math.max(redisBlockTimeoutMs, ackWaitBudgetMs);
        }
        return Math.min(joinTimeoutMs, MAX_JOIN_TIMEOUT_MS);
    }

    private void runPollLoop(long myGeneration) {
        boolean wasOffline = false;
        while (generation.get() == myGeneration && !stopping.get() && !listener.isStopping()) {
            try {
                pollOnce();
                if (wasOffline) {
                    logger.info("Recovered after " + consecutiveFailures + " consecutive failure(s) polling "
                            + "Redis queue " + queueKey);
                    updateStatusQuietly(com.boomi.connector.api.listen.ListenerStatus.ONLINE, null);
                    wasOffline = false;
                    consecutiveFailures = 0;
                }
            } catch (Exception e) {
                consecutiveFailures++;
                long now = System.currentTimeMillis();
                if (shouldLogFailure(consecutiveFailures, now, lastLoggedFailureAtMillis)) {
                    String throttleNote = consecutiveFailures > 1
                            ? " (" + consecutiveFailures + " consecutive failures; further identical failures "
                                    + "are logged at most once every "
                                    + TimeUnit.MILLISECONDS.toMinutes(FAILURE_LOG_THROTTLE_MS) + " minute(s))"
                            : "";
                    logger.log(Level.SEVERE, "Error while polling Redis queue " + queueKey + throttleNote, e);
                    lastLoggedFailureAtMillis = now;
                }
                updateStatusQuietly(com.boomi.connector.api.listen.ListenerStatus.OFFLINE, e.getMessage());
                wasOffline = true;
                // Reconnect on a fresh connection-level break (still-live-but-broken Jedis, e.g.
                // broken pipe) AND whenever the connection is currently null (a previous reconnect
                // attempt hasn't succeeded yet) -- the latter is what makes retrying self-sustaining
                // across repeated failures rather than only reacting once to the initial break.
                if (e instanceof JedisConnectionException || redisConnection == null) {
                    reconnectQuietly();
                }
                backoff();
            }
        }
    }

    /**
     * Decides whether this failure occurrence should be logged at SEVERE. Package-private (static,
     * pure) so the throttling decision is directly testable without waiting through real
     * {@link #backoff()} cycles. Always logs the first failure in a run; while consecutive failures
     * continue, throttles repeats to at most one per {@link #FAILURE_LOG_THROTTLE_MS}.
     */
    static boolean shouldLogFailure(int consecutiveFailureCount, long nowMillis, long lastLoggedAtMillis) {
        return consecutiveFailureCount <= 1 || (nowMillis - lastLoggedAtMillis) >= FAILURE_LOG_THROTTLE_MS;
    }

    /**
     * Replaces a broken {@link #redisConnection} after a {@link JedisConnectionException} (broken
     * pipe, read timeout, or Jedis's own "broken connection" fail-fast). Jedis never repairs a
     * broken {@code Connection} internally -- without this, the poll loop would retry the exact
     * same dead socket forever, which previously required a human to manually stop/start the
     * listener to recover (observed in production: ~7 hours of identical failures until a manual
     * restart). Mirrors {@link #start}'s own setup: builds the replacement into a local variable
     * first so a failed reconnect attempt (Redis still unreachable) never assigns a broken value,
     * and re-checks {@link #stopping} before publishing the new connection so a reconnect that
     * outlasts a concurrent {@code stop()}'s join timeout never leaks it.
     */
    private void reconnectQuietly() {
        RedisConnection old = this.redisConnection;
        this.redisConnection = null;
        if (old != null) {
            try {
                old.close();
            } catch (Exception closeError) {
                logger.log(Level.FINE, "Error closing broken Redis connection for " + queueKey, closeError);
            }
        }

        RedisConnection fresh = new RedisConnection(context);
        try {
            fresh.init();
            if (STREAM.equals(queueType)) {
                fresh.xgroupCreate(queueKey, consumerGroupName);
            }
        } catch (RuntimeException e) {
            fresh.close();
            logger.log(Level.FINE, "Reconnect attempt failed for " + queueKey, e);
            return;
        }

        if (stopping.get()) {
            fresh.close();
            return;
        }
        this.redisConnection = fresh;
    }

    /**
     * Calls {@link Listener#updateStatus}, logging and swallowing an {@link IllegalStateException}
     * rather than letting it kill the poll thread. The SDK documents {@code updateStatus} as
     * throwing this if called after the operation has been stopped -- a real possibility in a
     * shutdown race between {@code stop()} setting {@code stopping}/incrementing past this thread's
     * generation and this thread reaching an update call. Other exception types are not swallowed.
     */
    private void updateStatusQuietly(com.boomi.connector.api.listen.ListenerStatus status, String message) {
        try {
            if (message != null) {
                listener.updateStatus(status, message);
            } else {
                listener.updateStatus(status);
            }
        } catch (IllegalStateException e) {
            logger.log(Level.FINE, "Ignoring updateStatus(" + status + ") after the listener was stopped for "
                    + queueKey, e);
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
        RedisConnection connection = redisConnection;
        if (connection == null) {
            if (stopping.get()) {
                // Legitimate shutdown race: stop() already closed and nulled the connection while
                // this call was in flight. Not an error -- return quietly, matching the file's
                // established stop()-race guard pattern.
                logger.warning("Skipping list poll for " + queueKey + ": the Redis connection was "
                        + "already closed by stop().");
                return;
            }
            // NOT stopping: a previous reconnect attempt after a connection failure has not yet
            // succeeded. This must be signaled as a failure (not returned silently) so the caller's
            // failure counting/throttled logging/backoff/retry-reconnect logic keeps engaging --
            // silently "succeeding" here would falsely report the listener as recovered and skip
            // backoff() entirely, spinning in a tight loop.
            throw new ConnectorException("No Redis connection is currently available for queue " + queueKey
                    + " after a previous connection failure. A reconnect attempt will be retried.");
        }
        String value = connection.blpop(queueKey, pollBlockSeconds);
        if (value != null) {
            listener.submit(PayloadUtil.toPayload(toJson(queueKey, value)));
        }
    }

    private void pollStream() {
        RedisConnection connection = redisConnection;
        if (connection == null) {
            if (stopping.get()) {
                logger.warning("Skipping stream poll for " + queueKey + ": the Redis connection was "
                        + "already closed by stop().");
                return;
            }
            throw new ConnectorException("No Redis connection is currently available for queue " + queueKey
                    + " after a previous connection failure. A reconnect attempt will be retried.");
        }
        List<RedisStreamMessage> messages = connection.xreadGroup(
                queueKey, consumerGroupName, consumerName, 1, pollBlockSeconds * 1000);
        for (RedisStreamMessage message : messages) {
            submitStreamMessage(message);
        }

        long now = System.currentTimeMillis();
        if (now - lastReclaimSweepAt >= idleReclaimTimeoutMs) {
            runReclaimSweep();
            lastReclaimSweepAt = now;
        }
    }

    /**
     * Periodic idle-pending sweep for Stream mode, run synchronously as part of the poll thread's
     * loop (no new thread introduced -- just another step {@link #pollStream()} takes, gated by
     * elapsed time since the last sweep vs. {@code idle_reclaim_timeout_ms}). Messages claimed via
     * XCLAIM that are still under {@code max_delivery_attempts} are resubmitted through the
     * existing {@link #submitStreamMessage(RedisStreamMessage)} so ack-mode logic is not
     * duplicated; messages over the limit are moved to the configured dead-letter stream and
     * ack'd off the original pending list so they are not reclaimed again.
     *
     * <p><b>Bounding the sweep against {@code stop()}:</b> a sweep can process up to 100 pending
     * entries in one call, each potentially doing a real {@code xclaim} round-trip and, in
     * ack-on-completion mode (via {@link #submitStreamMessage(RedisStreamMessage)}), a nested
     * bounded {@code future.get()} wait. {@link #computeJoinTimeoutMs()} only budgets for ONE
     * {@code ackWaitTimeoutMs}-length wait, not a whole sweep's worth -- so once {@code stopping}
     * is signaled, the loop below checks it at the top of every iteration and bails before
     * starting a new entry. That bounds the sweep's remaining blocking time to at most one more
     * in-flight {@code xclaim}/{@code future.get()}, which the existing join budget already
     * covers. Every {@code redisConnection} access below is additionally taken through a local
     * snapshot with a null-guard, matching the pattern in {@link #submitStreamMessage}, since
     * {@code stop()} can still race ahead and tear down the connection while this method is
     * mid-iteration.
     */
    private void runReclaimSweep() {
        RedisConnection connection = redisConnection;
        if (connection == null) {
            logger.warning("Skipping reclaim sweep for " + queueKey + ": the Redis connection was "
                    + "already closed by stop().");
            return;
        }
        List<com.boomi.connector.redis.connection.RedisStreamPendingMessage> pending =
                connection.xpendingIdle(queueKey, consumerGroupName, idleReclaimTimeoutMs, 100);
        for (com.boomi.connector.redis.connection.RedisStreamPendingMessage p : pending) {
            if (stopping.get()) {
                logger.fine("Reclaim sweep for " + queueKey + " stopping early: shutdown was "
                        + "signaled mid-sweep; remaining pending entries will be picked up on a "
                        + "future sweep.");
                break;
            }

            connection = redisConnection;
            if (connection == null) {
                logger.warning("Aborting reclaim sweep for " + queueKey + ": the Redis connection "
                        + "was closed by stop() mid-sweep.");
                break;
            }

            RedisStreamMessage claimed = connection.xclaim(queueKey, consumerGroupName, consumerName, p.getId());
            if (claimed == null) {
                continue;
            }

            // XCLAIM increments Redis's delivery counter as a side effect of claiming, so the
            // count that governs THIS delivery is p.getDeliveryCount() + 1 (the pre-claim value
            // read from XPENDING above), not the pre-claim value itself -- comparing against the
            // stale pre-claim count let one extra delivery through past max_delivery_attempts.
            long deliveryCountAfterClaim = p.getDeliveryCount() + 1;
            if (deliveryCountAfterClaim > maxDeliveryAttempts) {
                connection = redisConnection;
                if (connection == null) {
                    logger.warning("Cannot dead-letter stream message " + claimed.getId() + " from "
                            + queueKey + ": the Redis connection was closed by stop() mid-sweep. "
                            + "The message remains pending and is eligible for a future reclaim.");
                    break;
                }
                connection.xadd(deadLetterKey, claimed.getValue());
                connection.xack(queueKey, consumerGroupName, claimed.getId());
                logger.warning("Dead-lettered stream message " + claimed.getId() + " from " + queueKey
                        + " after " + deliveryCountAfterClaim + " delivery attempts.");
            } else {
                submitStreamMessage(claimed);
            }
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
     * <p><b>Interaction with {@code stop()}'s join timeout:</b> {@link #computeJoinTimeoutMs()}
     * extends {@code stop()}'s {@code pollThread.join(...)} budget to also cover this bounded wait
     * when Stream mode + ack-on-completion is active, so {@code stop()} does not race ahead and
     * tear down {@code redisConnection} while this thread is still parked here under normal
     * timing. As defense in depth for the case where {@code stop()} is invoked with an
     * even-slower-than-expected process (or is itself given a shorter-than-expected caller-side
     * deadline), the success branch below null-checks {@code redisConnection} before calling
     * {@code xack} rather than assuming it is still present -- see the guard below.
     */
    void submitStreamMessage(RedisStreamMessage message) {
        String json = toJson(message.getId(), message.getValue());
        if (ackOnCompletion) {
            SubmitOptions options = new SubmitOptions().withWaitMode(WaitMode.PROCESS_COMPLETION);
            Future<ListenerExecutionResult> future = listener.submit(PayloadUtil.toPayload(json), options);
            try {
                ListenerExecutionResult result = future.get(ackWaitTimeoutMs, TimeUnit.MILLISECONDS);
                if (result.isSuccess()) {
                    // stop() may have raced ahead and torn down redisConnection while this thread
                    // was parked in future.get() above (e.g. stop() invoked with an
                    // even-slower-than-expected process, beyond what computeJoinTimeoutMs()
                    // budgeted for). Read the volatile field once into a local so the null-check
                    // and the use below observe the same value.
                    RedisConnection connection = redisConnection;
                    if (connection != null) {
                        connection.xack(queueKey, consumerGroupName, message.getId());
                    } else {
                        logger.warning("Cannot acknowledge stream message " + message.getId() + " on "
                                + queueKey + ": the Redis connection was already closed by stop() while "
                                + "this process execution was in flight. The message remains "
                                + "unacknowledged and is eligible for reclaim.");
                    }
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
            // stop() may have raced ahead and torn down redisConnection while listener.submit()
            // was in flight -- this ack-on-submission branch has no bounded wait like the
            // ack-on-completion branch above, but listener.submit() is not guaranteed to return
            // instantaneously. Read the volatile field once into a local so the null-check and the
            // use below observe the same value, matching the guard above.
            RedisConnection connection = redisConnection;
            if (connection != null) {
                connection.xack(queueKey, consumerGroupName, message.getId());
            } else {
                logger.warning("Cannot acknowledge stream message " + message.getId() + " on "
                        + queueKey + ": the Redis connection was already closed by stop() while "
                        + "listener.submit() was in flight. The message remains unacknowledged "
                        + "and is eligible for reclaim.");
            }
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
