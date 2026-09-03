package com.boomi.connector.redis.operation;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
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
    private final int concurrentExecutions;

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
    private volatile RedisConnection pollConnection;
    private volatile RedisConnection ackConnection;
    private final Object ackConnectionLock = new Object();
    private volatile boolean needsAckReconnect = false;
    private volatile ExecutorService workerPool;
    private volatile Semaphore executionPermits;
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

        long configuredConcurrency = longPropertyOrDefault(context.getOperationProperties(),
                "concurrent_executions", 1L);
        this.concurrentExecutions = (int) Math.max(1L, configuredConcurrency);

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

    long computeWorkerDrainTimeoutMsForTest() {
        return computeWorkerDrainTimeoutMs();
    }

    int getConcurrentExecutionsForTest() {
        return concurrentExecutions;
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
        // Same "build into locals first, only assign to instance fields on full success" discipline
        // as before -- now for a PAIR of connections. If ack.init() fails after poll.init() and
        // poll.xgroupCreate() already succeeded, both must still be closed rather than leaking the
        // half-built pair.
        RedisConnection poll = new RedisConnection(context);
        RedisConnection ack = null;
        try {
            poll.init();
            if (STREAM.equals(queueType)) {
                poll.xgroupCreate(queueKey, consumerGroupName);
                // List mode has no ack step and never gets a second connection at all.
                ack = new RedisConnection(context);
                ack.init();
            }
        } catch (RuntimeException e) {
            poll.close();
            if (ack != null) {
                ack.close();
            }
            throw e;
        }
        this.pollConnection = poll;
        this.ackConnection = ack;
        this.workerPool = Executors.newFixedThreadPool(concurrentExecutions, r -> {
            Thread t = new Thread(r, "Redis Listen Worker: " + queueKey);
            t.setDaemon(true);
            return t;
        });
        this.executionPermits = new Semaphore(concurrentExecutions);
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
            pollThread.interrupt();
            long joinTimeoutMs = computeJoinTimeoutMs();
            try {
                pollThread.join(joinTimeoutMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            pollThread = null;
        }
        ExecutorService pool = workerPool;
        if (pool != null) {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(computeWorkerDrainTimeoutMs(), TimeUnit.MILLISECONDS)) {
                    // A worker is still running past the drain budget (e.g. a pathological Future
                    // that ignores its own timeout) -- stop() must still return per the SDK contract,
                    // so abandon it rather than block indefinitely.
                    pool.shutdownNow();
                }
            } catch (InterruptedException e) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
            workerPool = null;
        }
        if (pollConnection != null) {
            pollConnection.close();
            pollConnection = null;
        }
        synchronized (ackConnectionLock) {
            if (ackConnection != null) {
                ackConnection.close();
                ackConnection = null;
            }
        }
    }

    /**
     * Computes how long {@link #stop()} should wait on {@code pollThread.join(...)}.
     *
     * <p>The base budget covers the Redis-side blocking call ({@code blpop}/{@code xreadGroup}),
     * sized from {@code pollBlockSeconds} with a floor of 10s and a 5s safety margin. The main
     * read loop's message dispatch now runs on {@link #workerPool} rather than this thread (see
     * {@link #computeWorkerDrainTimeoutMs()}) -- but {@link #runReclaimSweep()} still calls
     * {@link #submitStreamMessage(RedisStreamMessage)} SYNCHRONOUSLY on the poll thread for
     * redelivered entries under the delivery-attempt limit, and that method's
     * {@code future.get(ackWaitTimeoutMs, ...)} bound applies regardless of which thread calls
     * it. So in Stream mode with ack-on-completion, the poll thread itself can still be parked in
     * that wait via the reclaim sweep -- a second, independent source of in-flight blocking that
     * the base budget knows nothing about. When that mode is active, the join timeout must cover
     * whichever of the two the poll thread might currently be inside, so it is the max of both
     * components (each with its own 5s safety margin).
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

    /**
     * Computes how long {@link #stop()} should wait on {@code workerPool.awaitTermination(...)}.
     * The main read loop's dispatched workers (see {@link #dispatchStreamMessage}) can be parked
     * in {@code submitStreamMessage()}'s bounded {@code future.get(ackWaitTimeoutMs, ...)} call --
     * this budget covers that ASYNC, pool-dispatched wait, which is a separate call site from the
     * poll thread's own SYNCHRONOUS call to the same method via {@link #runReclaimSweep()} (still
     * covered by {@link #computeJoinTimeoutMs()}). Since workers run in parallel, this bound does
     * not need to grow with {@link #concurrentExecutions} -- every worker is independently bounded
     * by the same {@code ackWaitTimeoutMs}, so the slowest one still finishes within one such
     * window, not N of them stacked sequentially. Capped at {@link #MAX_JOIN_TIMEOUT_MS} for the
     * same reason {@link #computeJoinTimeoutMs()} is: the {@link #generation} counter, not this
     * timeout, is what actually prevents stale state from resurrecting.
     */
    private long computeWorkerDrainTimeoutMs() {
        long baseMs = Math.max(10_000L, (pollBlockSeconds * 1000L) + 5_000L);
        long drainTimeoutMs = baseMs;
        if (STREAM.equals(queueType) && ackOnCompletion) {
            long ackWaitBudgetMs = ackWaitTimeoutMs + 5_000L;
            drainTimeoutMs = Math.max(baseMs, ackWaitBudgetMs);
        }
        return Math.min(drainTimeoutMs, MAX_JOIN_TIMEOUT_MS);
    }

    private void runPollLoop(long myGeneration) {
        boolean wasOffline = false;
        while (generation.get() == myGeneration && !stopping.get() && !listener.isStopping()) {
            if (needsAckReconnect) {
                reconnectAckQuietly();
            }
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
                if (e instanceof JedisConnectionException || pollConnection == null) {
                    reconnectPollQuietly();
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
     * Replaces a broken {@link #pollConnection} after a {@link JedisConnectionException} (broken
     * pipe, read timeout, or Jedis's own "broken connection" fail-fast). Jedis never repairs a
     * broken {@code Connection} internally -- without this, the poll loop would retry the exact
     * same dead socket forever, which previously required a human to manually stop/start the
     * listener to recover (observed in production: ~7 hours of identical failures until a manual
     * restart). Mirrors {@link #start}'s own setup: builds the replacement into a local variable
     * first so a failed reconnect attempt (Redis still unreachable) never assigns a broken value,
     * and re-checks {@link #stopping} before publishing the new connection so a reconnect that
     * outlasts a concurrent {@code stop()}'s join timeout never leaks it.
     */
    private void reconnectPollQuietly() {
        RedisConnection old = this.pollConnection;
        this.pollConnection = null;
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
        this.pollConnection = fresh;
    }

    /**
     * Rebuilds {@link #ackConnection} after a worker (or the reclaim sweep) reported it broken via
     * {@link #needsAckReconnect}. Only the poll thread ever calls this -- workers only set the flag,
     * never reconnect themselves -- so there is never a race between two threads swapping
     * {@link #ackConnection} at once. Mirrors {@link #reconnectPollQuietly()}'s own discipline:
     * build into a local first, only publish on full success, and re-check {@link #stopping} before
     * publishing so a slow reconnect that outlasts a concurrent {@code stop()} never leaks it.
     *
     * <p>Publishing the fresh connection and clearing {@link #needsAckReconnect} happen inside the
     * SAME {@code synchronized} block: a worker could otherwise observe the freshly-published
     * connection, hit a genuine new failure, and set the flag again -- only for this method's
     * still-in-flight clear (running just after publish but before the flag write) to immediately
     * overwrite that fresh signal. Not a correctness gap either way (the next failure re-sets it),
     * but keeping both writes atomic avoids the avoidable race entirely.
     */
    private void reconnectAckQuietly() {
        RedisConnection old;
        synchronized (ackConnectionLock) {
            old = ackConnection;
            ackConnection = null;
        }
        if (old != null) {
            try {
                old.close();
            } catch (Exception closeError) {
                logger.log(Level.FINE, "Error closing broken ack connection for " + queueKey, closeError);
            }
        }

        RedisConnection fresh = new RedisConnection(context);
        try {
            fresh.init();
        } catch (RuntimeException e) {
            fresh.close();
            logger.log(Level.FINE, "Ack connection reconnect attempt failed for " + queueKey, e);
            return;
        }

        if (stopping.get()) {
            fresh.close();
            return;
        }
        synchronized (ackConnectionLock) {
            ackConnection = fresh;
            needsAckReconnect = false;
        }
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
        RedisConnection connection = pollConnection;
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
        // Captured locally (not re-read from the volatile field later) so that a stale-generation
        // thread's eventual release()/submit() targets the SAME Semaphore/ExecutorService
        // instances it acquired from, even if a concurrent start()/stop() cycle has since replaced
        // executionPermits/workerPool with new instances for the current generation -- releasing
        // to the wrong Semaphore would silently corrupt its permit count, and submitting to the
        // wrong (newer generation's) pool would run this stale thread's work on the wrong pool.
        Semaphore permits = executionPermits;
        ExecutorService pool = workerPool;
        if (!acquirePermit(permits)) {
            // Interrupted while waiting for a free execution slot -- this only happens during
            // shutdown (stop() interrupts this thread). Nothing was read from Redis, so nothing to
            // release; the outer loop's condition check will observe stopping/isStopping() and exit.
            return;
        }
        String value;
        try {
            value = connection.blpop(queueKey, pollBlockSeconds);
        } catch (RuntimeException e) {
            permits.release();
            throw e;
        }
        if (value == null) {
            permits.release();
            return;
        }
        dispatchListItem(value, pool, permits);
    }

    private void dispatchListItem(String value, ExecutorService pool, Semaphore permits) {
        if (pool == null) {
            // stop() closed and nulled the pool while this poll cycle's blpop() call was still in
            // flight (e.g. a call that outlasted the join/drain timeouts). Nothing to submit to --
            // release the permit and drop the value rather than raw-NPE on pool.submit().
            logger.warning("Skipping dispatch for a popped value from " + queueKey + ": the worker "
                    + "pool was already closed by stop().");
            permits.release();
            return;
        }
        try {
            pool.submit(() -> {
                try {
                    listener.submit(PayloadUtil.toPayload(toJson(queueKey, value)));
                } finally {
                    permits.release();
                }
            });
        } catch (RejectedExecutionException e) {
            // The pool was shut down between this method's null-check above and this submit()
            // call -- a narrow but real race with stop(). Same recovery as the null-pool case.
            logger.warning("Skipping dispatch for a popped value from " + queueKey + ": the worker "
                    + "pool was shut down by stop() before the task could be submitted.");
            permits.release();
        }
    }

    private boolean acquirePermit(Semaphore permits) {
        try {
            permits.acquire();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void pollStream() {
        RedisConnection connection = pollConnection;
        if (connection == null) {
            if (stopping.get()) {
                logger.warning("Skipping stream poll for " + queueKey + ": the Redis connection was "
                        + "already closed by stop().");
                return;
            }
            throw new ConnectorException("No Redis connection is currently available for queue " + queueKey
                    + " after a previous connection failure. A reconnect attempt will be retried.");
        }
        Semaphore permits = executionPermits;
        ExecutorService pool = workerPool;
        if (!acquirePermit(permits)) {
            return;
        }
        List<RedisStreamMessage> messages;
        try {
            messages = connection.xreadGroup(
                    queueKey, consumerGroupName, consumerName, 1, pollBlockSeconds * 1000);
        } catch (RuntimeException e) {
            permits.release();
            throw e;
        }
        if (messages.isEmpty()) {
            permits.release();
        } else {
            for (RedisStreamMessage message : messages) {
                dispatchStreamMessage(message, pool, permits);
            }
        }

        long now = System.currentTimeMillis();
        if (now - lastReclaimSweepAt >= idleReclaimTimeoutMs) {
            // Unchanged from before Task 3: the reclaim sweep runs synchronously on the poll thread,
            // not dispatched to the worker pool, and its own submitStreamMessage() calls (for
            // redelivered entries under the attempt limit) do not interact with executionPermits --
            // that accounting only governs the main read-loop's dispatch path above.
            runReclaimSweep();
            lastReclaimSweepAt = now;
        }
    }

    private void dispatchStreamMessage(RedisStreamMessage message, ExecutorService pool, Semaphore permits) {
        if (pool == null) {
            // stop() closed and nulled the pool while this poll cycle's xreadGroup() call was
            // still in flight. Nothing to submit to -- release the permit and drop the message
            // rather than raw-NPE on pool.submit(). The message stays unacknowledged in Redis and
            // is eligible for reclaim.
            logger.warning("Skipping dispatch for stream message " + message.getId() + " on " + queueKey
                    + ": the worker pool was already closed by stop().");
            permits.release();
            return;
        }
        try {
            pool.submit(() -> {
                try {
                    submitStreamMessage(message);
                } finally {
                    permits.release();
                }
            });
        } catch (RejectedExecutionException e) {
            // The pool was shut down between this method's null-check above and this submit()
            // call -- a narrow but real race with stop(). Same recovery as the null-pool case.
            logger.warning("Skipping dispatch for stream message " + message.getId() + " on " + queueKey
                    + ": the worker pool was shut down by stop() before the task could be submitted.");
            permits.release();
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
        RedisConnection poll = pollConnection;
        if (poll == null) {
            logger.warning("Skipping reclaim sweep for " + queueKey + ": the Redis connection was "
                    + "already closed by stop().");
            return;
        }
        List<com.boomi.connector.redis.connection.RedisStreamPendingMessage> pending =
                poll.xpendingIdle(queueKey, consumerGroupName, idleReclaimTimeoutMs, 100);
        for (com.boomi.connector.redis.connection.RedisStreamPendingMessage p : pending) {
            if (stopping.get()) {
                logger.fine("Reclaim sweep for " + queueKey + " stopping early: shutdown was "
                        + "signaled mid-sweep; remaining pending entries will be picked up on a "
                        + "future sweep.");
                break;
            }

            RedisStreamMessage claimed = claimQuietly(p.getId());
            if (claimed == null) {
                continue;
            }

            // XCLAIM increments Redis's delivery counter as a side effect of claiming, so the
            // count that governs THIS delivery is p.getDeliveryCount() + 1 (the pre-claim value
            // read from XPENDING above), not the pre-claim value itself -- comparing against the
            // stale pre-claim count let one extra delivery through past max_delivery_attempts.
            long deliveryCountAfterClaim = p.getDeliveryCount() + 1;
            if (deliveryCountAfterClaim > maxDeliveryAttempts) {
                deadLetterQuietly(claimed);
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
                    ackQuietly(message.getId());
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
            ackQuietly(message.getId());
        }
    }

    /**
     * Acknowledges one Stream message under {@link #ackConnectionLock}. A single Jedis connection
     * is not thread-safe, and once Task 3 introduces a worker pool, multiple workers can reach this
     * call concurrently -- the lock serializes every {@code ackConnection} command so no two
     * commands are ever in flight on the same socket at once.
     */
    private void ackQuietly(String messageId) {
        synchronized (ackConnectionLock) {
            RedisConnection connection = ackConnection;
            if (connection == null) {
                logger.warning("Cannot acknowledge stream message " + messageId + " on " + queueKey
                        + ": the ack connection was already closed by stop() or is being reconnected. "
                        + "The message remains unacknowledged and is eligible for reclaim.");
                return;
            }
            try {
                connection.xack(queueKey, consumerGroupName, messageId);
            } catch (JedisConnectionException e) {
                needsAckReconnect = true;
                logger.log(Level.WARNING, "Ack connection broke while acknowledging stream message "
                        + messageId + " on " + queueKey + "; a reconnect will be attempted on the "
                        + "next poll iteration.", e);
            }
        }
    }

    /**
     * Claims one idle-pending Stream entry during {@link #runReclaimSweep()}. Guarded the same way
     * as {@link #ackQuietly}: a {@link JedisConnectionException} here means the ack connection --
     * not the poll connection driving the sweep -- has broken, so it only sets
     * {@link #needsAckReconnect} rather than reconnecting inline (only the poll thread reconnects;
     * see {@link #reconnectAckQuietly()}). Returns {@code null} on either a missing connection or a
     * failed claim so the sweep's caller treats both the same way: skip this entry, pick it up on a
     * future sweep.
     */
    private RedisStreamMessage claimQuietly(String messageId) {
        synchronized (ackConnectionLock) {
            RedisConnection connection = ackConnection;
            if (connection == null) {
                logger.warning("Cannot claim pending stream message " + messageId + " from " + queueKey
                        + ": the ack connection was already closed by stop() or is being reconnected.");
                return null;
            }
            try {
                return connection.xclaim(queueKey, consumerGroupName, consumerName, messageId);
            } catch (JedisConnectionException e) {
                needsAckReconnect = true;
                logger.log(Level.WARNING, "Ack connection broke while claiming pending stream message "
                        + messageId + " on " + queueKey + "; a reconnect will be attempted on the "
                        + "next poll iteration.", e);
                return null;
            }
        }
    }

    /**
     * Moves one over-limit claimed message to the dead-letter stream and acks it off the original
     * pending list, during {@link #runReclaimSweep()}. Same ack-connection guard discipline as
     * {@link #claimQuietly}.
     */
    private void deadLetterQuietly(RedisStreamMessage claimed) {
        synchronized (ackConnectionLock) {
            RedisConnection connection = ackConnection;
            if (connection == null) {
                logger.warning("Cannot dead-letter stream message " + claimed.getId() + " from " + queueKey
                        + ": the ack connection was closed by stop() or is being reconnected. The message "
                        + "remains pending and is eligible for a future reclaim.");
                return;
            }
            try {
                connection.xadd(deadLetterKey, claimed.getValue());
                connection.xack(queueKey, consumerGroupName, claimed.getId());
            } catch (JedisConnectionException e) {
                needsAckReconnect = true;
                logger.log(Level.WARNING, "Ack connection broke while dead-lettering stream message "
                        + claimed.getId() + " on " + queueKey + "; a reconnect will be attempted on "
                        + "the next poll iteration.", e);
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
