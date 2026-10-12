package com.boomi.connector.redis;

import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.listen.Listener;
import com.boomi.connector.api.listen.ListenerExecutionResult;
import com.boomi.connector.api.listen.ListenerStatus;
import com.boomi.connector.api.listen.SubmitOptions;
import com.boomi.connector.redis.operation.RedisListenOperation;
import com.boomi.connector.redis.pool.RedisClientPoolManager;
import com.boomi.connector.redis.pool.RedisListenManager;
import com.boomi.connector.testutil.SimpleOperationContext;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end coverage for {@link RedisListenOperation} against a real {@code redis:7} container:
 * List-mode send -&gt; listen, Stream-mode send -&gt; listen with ack-on-completion, and
 * dead-lettering once a poison message crosses {@code max_delivery_attempts}. Fully self-contained
 * (same pattern as {@link RedisEntraPoolingIT} / {@link RedisSslOperationIT}): no properties
 * files, no external Redis, no credentials on disk.
 *
 * <p>The operation is driven directly (constructed with a real, Map-backed
 * {@link SimpleOperationContext} rather than a hand-stubbed mock) since {@code ListenOperation}s
 * are not exercised through {@code ConnectorTester}'s GET/UPSERT/DELETE-oriented API. The
 * {@link Listener} contract itself is mocked, since {@code Listener} instances are supplied by the
 * Boomi runtime and connector developers are explicitly told not to implement it themselves.
 */
@Category(IntegrationTest.class)
public class RedisListenSendIT {

    @SuppressWarnings("resource")
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379);

    private static String hostPort;

    @BeforeClass
    public static void startRedis() {
        REDIS.start();
        hostPort = REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    }

    @AfterClass
    public static void stopRedis() {
        REDIS.stop();
    }

    @Before
    public void resetSharedPoolsBefore() {
        RedisClientPoolManager.closeAll();
    }

    @After
    public void resetSharedPoolsAfter() {
        RedisClientPoolManager.closeAll();
    }

    private Jedis directClient() {
        return new Jedis(new HostAndPort(REDIS.getHost(), REDIS.getMappedPort(6379)));
    }

    /**
     * Builds a real (non-mocked) {@link OperationContext} backed by the SDK test-util's
     * Map-backed property maps, so {@code getLongProperty}/{@code getBooleanProperty} defaults
     * behave exactly like production Boomi rather than requiring every call site to be stubbed.
     */
    private OperationContext listenContext(String queueType, String queueKey, String consumerGroupName,
            String ackMode, long idleReclaimTimeoutMs, long maxDeliveryAttempts, long pollBlockMs) {
        return listenContext(queueType, queueKey, consumerGroupName, ackMode, idleReclaimTimeoutMs,
                maxDeliveryAttempts, pollBlockMs, 1L);
    }

    private OperationContext listenContext(String queueType, String queueKey, String consumerGroupName,
            String ackMode, long idleReclaimTimeoutMs, long maxDeliveryAttempts, long pollBlockMs,
            long concurrentExecutions) {
        return listenContext(queueType, queueKey, consumerGroupName, ackMode, idleReclaimTimeoutMs,
                maxDeliveryAttempts, pollBlockMs, concurrentExecutions, 5L, 5L);
    }

    private OperationContext listenContext(String queueType, String queueKey, String consumerGroupName,
            String ackMode, long idleReclaimTimeoutMs, long maxDeliveryAttempts, long pollBlockMs,
            long concurrentExecutions, long connectionTimeoutSeconds, long socketTimeoutSeconds) {
        Map<String, Object> connProps = new HashMap<>();
        connProps.put("hosts", hostPort);
        connProps.put("authenticationType", "None");
        connProps.put("clusteringPolicy", "NonClustered");
        connProps.put("useSSL", false);
        connProps.put("poolEnabled", false);
        connProps.put("connectionTimeout", connectionTimeoutSeconds);
        connProps.put("socketTimeout", socketTimeoutSeconds);

        Map<String, Object> opProps = new HashMap<>();
        opProps.put("queue_type", queueType);
        opProps.put("queue_key", queueKey);
        if (consumerGroupName != null && !consumerGroupName.isEmpty()) {
            opProps.put("consumer_group_name", consumerGroupName);
        }
        opProps.put("singleton_mode", false);
        opProps.put("poll_block_ms", pollBlockMs);
        if (ackMode != null) {
            opProps.put("ack_mode", ackMode);
        }
        opProps.put("idle_reclaim_timeout_ms", idleReclaimTimeoutMs);
        opProps.put("max_delivery_attempts", maxDeliveryAttempts);
        opProps.put("concurrent_executions", concurrentExecutions);

        return new SimpleOperationContext(null, null, OperationType.LISTEN, connProps, opProps, "listen", null);
    }

    private static String readPayload(Payload payload) throws IOException {
        InputStream in = payload.readFrom();
        if (in != null) {
            try {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                byte[] chunk = new byte[1024];
                int n;
                while ((n = in.read(chunk)) != -1) {
                    buffer.write(chunk, 0, n);
                }
                return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
            } finally {
                in.close();
            }
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        payload.writeTo(out);
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }

    @Test
    public void listModeEndToEndSendThenListen() throws Exception {
        String queueKey = "it-list-" + System.nanoTime();
        try (Jedis direct = directClient()) {
            direct.rpush(queueKey, "hello-list");
        }

        OperationContext ctx = listenContext("List", queueKey, null, null, 60_000L, 5L, 1000L);
        RedisListenOperation op = new RedisListenOperation(ctx);

        List<String> received = new CopyOnWriteArrayList<>();
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);
        org.mockito.Mockito.doAnswer(invocation -> {
            Payload payload = invocation.getArgument(0);
            received.add(readPayload(payload));
            return null;
        }).when(listener).submit(any(Payload.class));

        op.start(listener, new RedisListenManager());
        try {
            long deadline = System.currentTimeMillis() + 10_000L;
            while (received.isEmpty() && System.currentTimeMillis() < deadline) {
                TimeUnit.MILLISECONDS.sleep(100);
            }
        } finally {
            op.stop();
        }

        assertFalse("Expected the List-mode listener to submit the pushed message", received.isEmpty());
        assertTrue("Expected the submitted payload to contain the pushed value; got: " + received.get(0),
                received.get(0).contains("hello-list"));
    }

    @Test
    public void streamModeEndToEndSendThenListenWithAckOnCompletion() throws Exception {
        String queueKey = "it-stream-" + System.nanoTime();
        String group = "it-group-" + System.nanoTime();

        OperationContext ctx = listenContext("Stream", queueKey, group, "OnCompletion", 60_000L, 5L, 1000L);
        RedisListenOperation op = new RedisListenOperation(ctx);

        List<String> received = new CopyOnWriteArrayList<>();
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);
        ListenerExecutionResult success = mock(ListenerExecutionResult.class);
        when(success.isSuccess()).thenReturn(true);
        when(listener.submit(any(Payload.class), any(SubmitOptions.class))).thenAnswer(invocation -> {
            Payload payload = invocation.getArgument(0);
            received.add(readPayload(payload));
            return CompletableFuture.completedFuture(success);
        });

        op.start(listener, new RedisListenManager());
        try (Jedis direct = directClient()) {
            direct.xadd(queueKey, StreamEntryID.NEW_ENTRY, Collections.singletonMap("value", "hello-stream"));

            long deadline = System.currentTimeMillis() + 15_000L;
            while (received.isEmpty() && System.currentTimeMillis() < deadline) {
                TimeUnit.MILLISECONDS.sleep(100);
            }
            assertFalse("Expected the Stream-mode listener to receive the added message", received.isEmpty());
            assertTrue("Expected the submitted payload to contain the added value; got: " + received.get(0),
                    received.get(0).contains("hello-stream"));

            // The mocked future above already completed successfully by the time submit()
            // returned, so the ack should follow immediately - poll briefly for it to land.
            long ackDeadline = System.currentTimeMillis() + 5_000L;
            long pendingTotal = direct.xpending(queueKey, group).getTotal();
            while (pendingTotal > 0 && System.currentTimeMillis() < ackDeadline) {
                TimeUnit.MILLISECONDS.sleep(100);
                pendingTotal = direct.xpending(queueKey, group).getTotal();
            }
            assertEquals("Expected the stream message to be fully acknowledged after ack-on-completion",
                    0L, direct.xpending(queueKey, group).getTotal());
        } finally {
            op.stop();
        }
    }

    @Test
    public void listenerReconnectsAfterItsConnectionIsForciblyKilled() throws Exception {
        // Reproduces a real production incident: a listener's live connection died (broken pipe /
        // connection reset -- here simulated by forcibly killing it server-side, same technique as
        // RedisEntraPoolingIT.tokenRotationSelfHealsAfterConnectionKill), and before this fix the
        // poll loop retried that same dead connection forever (~4800 identical failures logged over
        // ~7 hours) until a human manually stopped/started the listener. This proves the listener
        // now heals itself: it notices the break, reconnects, and still delivers a message sent
        // afterward, with no manual intervention.
        String queueKey = "it-selfheal-" + System.nanoTime();

        OperationContext ctx = listenContext("List", queueKey, null, null, 60_000L, 5L, 1000L);
        RedisListenOperation op = new RedisListenOperation(ctx);

        List<String> received = new CopyOnWriteArrayList<>();
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);
        org.mockito.Mockito.doAnswer(invocation -> {
            Payload payload = invocation.getArgument(0);
            received.add(readPayload(payload));
            return null;
        }).when(listener).submit(any(Payload.class));

        op.start(listener, new RedisListenManager());
        try {
            // Let the listener settle into its BLPOP loop, then sever every client connection
            // server-side. execInContainer runs redis-cli inside the container itself, so no
            // external redis-cli binary or auth is needed against this unauthenticated instance.
            Thread.sleep(500);
            org.testcontainers.containers.Container.ExecResult killResult =
                    REDIS.execInContainer("redis-cli", "CLIENT", "KILL", "TYPE", "normal");
            assertEquals("redis-cli CLIENT KILL failed: " + killResult.getStderr(), 0, killResult.getExitCode());

            try (Jedis direct = directClient()) {
                direct.rpush(queueKey, "hello-after-kill");
            }

            long deadline = System.currentTimeMillis() + 20_000L;
            while (received.isEmpty() && System.currentTimeMillis() < deadline) {
                TimeUnit.MILLISECONDS.sleep(200);
            }
        } finally {
            op.stop();
        }

        assertFalse("Expected the listener to self-heal after its connection was killed and still "
                + "deliver a message sent afterward", received.isEmpty());
        assertTrue("Expected the delivered payload to contain the post-kill value; got: " + received.get(0),
                received.get(0).contains("hello-after-kill"));
    }

    @Test
    public void ackConnectionSelfHealsAfterItsConnectionIsForciblyKilled() throws Exception {
        // The ack-connection analog of listenerReconnectsAfterItsConnectionIsForciblyKilled above:
        // ackConnection can break independently of pollConnection (e.g. a worker's xack hits a dead
        // socket while the poll connection is still healthy). CLIENT KILL TYPE normal severs EVERY
        // client connection server-side, so it breaks both the poll and ack connections at once here
        // -- proving self-healing requires the message sent afterward to be both delivered (poll
        // connection healed) AND fully acknowledged (ack connection healed too), with no manual
        // intervention. A short idle_reclaim_timeout_ms ensures that even if the very first
        // post-kill ack attempt loses the race against reconnectAckQuietly() and is dropped, the
        // idle-reclaim sweep picks the still-pending message back up and re-submits/re-acks it once
        // the ack connection is back.
        String queueKey = "it-ack-selfheal-" + System.nanoTime();
        String group = "it-ack-selfheal-group-" + System.nanoTime();
        long idleReclaimTimeoutMs = 3000L;

        OperationContext ctx = listenContext("Stream", queueKey, group, "OnCompletion",
                idleReclaimTimeoutMs, 5L, 1000L);
        RedisListenOperation op = new RedisListenOperation(ctx);

        List<String> received = new CopyOnWriteArrayList<>();
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);
        ListenerExecutionResult success = mock(ListenerExecutionResult.class);
        when(success.isSuccess()).thenReturn(true);
        when(listener.submit(any(Payload.class), any(SubmitOptions.class))).thenAnswer(invocation -> {
            Payload payload = invocation.getArgument(0);
            received.add(readPayload(payload));
            return CompletableFuture.completedFuture(success);
        });

        op.start(listener, new RedisListenManager());
        try {
            // Let the listener settle into its XREADGROUP loop, then sever every client connection
            // server-side -- both the poll and ack connections are "normal" clients.
            Thread.sleep(500);
            org.testcontainers.containers.Container.ExecResult killResult =
                    REDIS.execInContainer("redis-cli", "CLIENT", "KILL", "TYPE", "normal");
            assertEquals("redis-cli CLIENT KILL failed: " + killResult.getStderr(), 0, killResult.getExitCode());

            try (Jedis direct = directClient()) {
                direct.xadd(queueKey, StreamEntryID.NEW_ENTRY,
                        Collections.singletonMap("value", "hello-after-kill"));

                long deliveredDeadline = System.currentTimeMillis() + 20_000L;
                while (received.isEmpty() && System.currentTimeMillis() < deliveredDeadline) {
                    TimeUnit.MILLISECONDS.sleep(200);
                }
                assertFalse("Expected the listener to self-heal its poll connection and still "
                        + "deliver a message sent after the kill", received.isEmpty());
                assertTrue("Expected the delivered payload to contain the post-kill value; got: "
                        + received.get(0), received.get(0).contains("hello-after-kill"));

                // Full acknowledgment can only happen once the ack connection has been rebuilt (it
                // was killed too) -- either directly via the worker's own xack, or via the
                // idle-reclaim sweep re-claiming and re-submitting the message after
                // idle_reclaim_timeout_ms if the first attempt raced a still-broken ack connection.
                long ackDeadline = System.currentTimeMillis() + 30_000L;
                long pendingTotal = direct.xpending(queueKey, group).getTotal();
                while (pendingTotal > 0 && System.currentTimeMillis() < ackDeadline) {
                    TimeUnit.MILLISECONDS.sleep(300);
                    pendingTotal = direct.xpending(queueKey, group).getTotal();
                }
                assertEquals("Expected the stream message to be fully acknowledged once the ack "
                        + "connection self-heals", 0L, direct.xpending(queueKey, group).getTotal());
            }
        } finally {
            op.stop();
        }
    }

    @Test
    public void deadLettersAfterMaxDeliveryAttempts() throws Exception {
        String queueKey = "it-dlq-" + System.nanoTime();
        String group = "it-dlq-group-" + System.nanoTime();
        String deadLetterKey = queueKey + ":dead-letter";
        long idleReclaimTimeoutMs = 1000L;
        long maxDeliveryAttempts = 2L;

        try (Jedis direct = directClient()) {
            direct.xgroupCreate(queueKey, group, new StreamEntryID(0), true);
            direct.xadd(queueKey, StreamEntryID.NEW_ENTRY, Collections.singletonMap("value", "poison"));
            // Deliver it into the group's PEL once via a throwaway consumer, without acking, so it
            // starts life already pending (delivery count 1) before our operation's own consumer
            // ever sees it. XREADGROUP with ">" only returns entries never delivered to any
            // consumer in the group, so our operation's own poll loop will never see this entry as
            // "new" - it can only reach it through the idle-reclaim sweep's XCLAIM, which is what
            // lets this test observe/control delivery-count growth toward the dead-letter
            // threshold.
            direct.xreadGroup(group, "seed-consumer", XReadGroupParams.xReadGroupParams().count(1),
                    Collections.singletonMap(queueKey, StreamEntryID.UNRECEIVED_ENTRY));
        }

        OperationContext ctx = listenContext("Stream", queueKey, group, "OnCompletion",
                idleReclaimTimeoutMs, maxDeliveryAttempts, 1000L);
        RedisListenOperation op = new RedisListenOperation(ctx);

        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);
        // Every ack-on-completion submission "hangs" (a Future that never completes), so
        // submitStreamMessage()'s bounded future.get(ackWaitTimeoutMs, ...) always times out and
        // the message is never XACKed. It stays pending, ages past idle_reclaim_timeout_ms again,
        // and is re-XCLAIMed (incrementing its Redis-tracked delivery count) on each sweep until
        // that count crosses max_delivery_attempts and the sweep dead-letters it.
        when(listener.submit(any(Payload.class), any(SubmitOptions.class)))
                .thenAnswer(invocation -> new CompletableFuture<ListenerExecutionResult>());

        op.start(listener, new RedisListenManager());
        try (Jedis direct = directClient()) {
            long deadline = System.currentTimeMillis() + 30_000L;
            long dlqLen = 0;
            while (dlqLen == 0 && System.currentTimeMillis() < deadline) {
                TimeUnit.MILLISECONDS.sleep(300);
                dlqLen = direct.exists(deadLetterKey) ? direct.xlen(deadLetterKey) : 0;
            }

            assertTrue("Expected the poison message to be dead-lettered", direct.exists(deadLetterKey));
            assertEquals(1L, direct.xlen(deadLetterKey));
            assertEquals("Expected the original pending entry to be ack'd off after dead-lettering",
                    0L, direct.xpending(queueKey, group).getTotal());
        } finally {
            op.stop();
        }
    }

    @Test
    public void concurrentExecutionsAllowsOverlappingExecutionsInListMode() throws Exception {
        String queueKey = "it-concurrency-list-" + System.nanoTime();
        int messageCount = 6;
        int concurrentExecutions = 3;
        try (Jedis direct = directClient()) {
            for (int i = 0; i < messageCount; i++) {
                direct.rpush(queueKey, "msg-" + i);
            }
        }

        OperationContext ctx = listenContext("List", queueKey, null, null, 60_000L, 5L, 500L,
                concurrentExecutions);
        RedisListenOperation op = new RedisListenOperation(ctx);

        List<String> received = new CopyOnWriteArrayList<>();
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);
        org.mockito.Mockito.doAnswer(invocation -> {
            Payload payload = invocation.getArgument(0);
            Thread.sleep(1000L);
            received.add(readPayload(payload));
            return null;
        }).when(listener).submit(any(Payload.class));

        long startedAt = System.currentTimeMillis();
        op.start(listener, new RedisListenManager());
        try {
            long deadline = System.currentTimeMillis() + 20_000L;
            while (received.size() < messageCount && System.currentTimeMillis() < deadline) {
                TimeUnit.MILLISECONDS.sleep(100);
            }
        } finally {
            op.stop();
        }
        long elapsedMs = System.currentTimeMillis() - startedAt;

        assertEquals("Expected all pushed messages to be delivered", messageCount, received.size());
        // Sequential (concurrency=1) would take >= 6 * 1000ms = 6000ms. With 3-way concurrency, 6
        // messages each taking 1000ms complete in 2 batches. Generous ceiling still clearly
        // distinguishes "ran concurrently" from "ran sequentially" without being flaky on a loaded
        // CI box.
        assertTrue("Expected overlapping execution to finish well under the fully-sequential bound "
                + "of 6000ms -- elapsed=" + elapsedMs + "ms", elapsedMs < 5000L);
    }

    @Test
    public void concurrentExecutionsPreservesAckOnCompletionCorrectnessUnderLoad() throws Exception {
        String queueKey = "it-concurrency-stream-" + System.nanoTime();
        String group = "it-concurrency-group-" + System.nanoTime();
        int messageCount = 10;
        int concurrentExecutions = 4;

        OperationContext ctx = listenContext("Stream", queueKey, group, "OnCompletion", 60_000L, 5L, 500L,
                concurrentExecutions);
        RedisListenOperation op = new RedisListenOperation(ctx);

        List<String> received = new CopyOnWriteArrayList<>();
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);
        ListenerExecutionResult success = mock(ListenerExecutionResult.class);
        when(success.isSuccess()).thenReturn(true);
        when(listener.submit(any(Payload.class), any(SubmitOptions.class))).thenAnswer(invocation -> {
            Payload payload = invocation.getArgument(0);
            Thread.sleep(200L);
            received.add(readPayload(payload));
            return CompletableFuture.completedFuture(success);
        });

        op.start(listener, new RedisListenManager());
        try (Jedis direct = directClient()) {
            for (int i = 0; i < messageCount; i++) {
                direct.xadd(queueKey, StreamEntryID.NEW_ENTRY, Collections.singletonMap("value", "m" + i));
            }

            long deadline = System.currentTimeMillis() + 30_000L;
            while (received.size() < messageCount && System.currentTimeMillis() < deadline) {
                TimeUnit.MILLISECONDS.sleep(100);
            }
            assertEquals("Expected every message delivered exactly once, no duplicates", messageCount,
                    received.size());
            assertEquals("Expected every delivered value to be distinct", messageCount,
                    new java.util.HashSet<>(received).size());

            long ackDeadline = System.currentTimeMillis() + 10_000L;
            long pendingTotal = direct.xpending(queueKey, group).getTotal();
            while (pendingTotal > 0 && System.currentTimeMillis() < ackDeadline) {
                TimeUnit.MILLISECONDS.sleep(100);
                pendingTotal = direct.xpending(queueKey, group).getTotal();
            }
            assertEquals("Expected every message fully acknowledged despite concurrent execution",
                    0L, direct.xpending(queueKey, group).getTotal());
        } finally {
            op.stop();
        }
    }

    @Test
    public void stopDuringConcurrentExecutionsDrainsInFlightWorkersBeforeClosingConnections() throws Exception {
        String queueKey = "it-concurrency-drain-" + System.nanoTime();
        String group = "it-concurrency-drain-group-" + System.nanoTime();
        int concurrentExecutions = 2;

        OperationContext ctx = listenContext("Stream", queueKey, group, "OnCompletion", 60_000L, 5L, 500L,
                concurrentExecutions);
        RedisListenOperation op = new RedisListenOperation(ctx);

        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);
        ListenerExecutionResult success = mock(ListenerExecutionResult.class);
        when(success.isSuccess()).thenReturn(true);
        when(listener.submit(any(Payload.class), any(SubmitOptions.class))).thenAnswer(invocation -> {
            // 4s (not 2s) so the margin above the >= 1000ms assertion below stays wide even when
            // the 700ms pre-stop() sleep overshoots badly on a loaded box: at 2s, a sleep that
            // overshot to ~1200ms left under 1000ms of remaining worker time and the assertion
            // could fail against a perfectly correct implementation.
            Thread.sleep(4000L);
            return CompletableFuture.completedFuture(success);
        });

        op.start(listener, new RedisListenManager());
        long stopElapsedMs;
        try {
            try (Jedis direct = directClient()) {
                direct.xadd(queueKey, StreamEntryID.NEW_ENTRY, Collections.singletonMap("value", "slow"));
                // Give the poll thread time to read the message and dispatch it to a worker, which
                // is now ~3.3s away from finishing its 4s submit() call, before stop() races it.
                Thread.sleep(700L);
            }
        } finally {
            long stopStartedAt = System.currentTimeMillis();
            op.stop();
            stopElapsedMs = System.currentTimeMillis() - stopStartedAt;
        }

        assertTrue("Expected stop() to wait for the in-flight ~4s worker execution to finish before "
                + "returning -- elapsed=" + stopElapsedMs + "ms", stopElapsedMs >= 1000L);

        try (Jedis direct = directClient()) {
            long ackDeadline = System.currentTimeMillis() + 5_000L;
            long pendingTotal = direct.xpending(queueKey, group).getTotal();
            while (pendingTotal > 0 && System.currentTimeMillis() < ackDeadline) {
                TimeUnit.MILLISECONDS.sleep(100);
                pendingTotal = direct.xpending(queueKey, group).getTotal();
            }
            assertEquals("Expected the in-flight message to be acknowledged since its worker was "
                    + "allowed to finish (and ack) before stop() closed the ack connection", 0L,
                    direct.xpending(queueKey, group).getTotal());
        }
    }

    /**
     * Proves the runtime is told the listener is unhealthy when Redis genuinely goes away, and
     * told it recovered when Redis returns -- using a real fault, not a mocked exception.
     * {@code docker pause} freezes the Redis process while its mapped port stays the same (a
     * stop/start would remap the port and strand the shared {@code hostPort}); the poll
     * connection's bounded blocking read then times out, the listener reports OFFLINE with a
     * non-blank message, and after unpause the reconnect path reports ONLINE and delivers a
     * message pushed while the listener was recovering.
     */
    @Test(timeout = 90_000L)
    public void listenerReportsOfflineWhenRedisIsUnreachableThenOnlineAfterRecovery() throws Exception {
        String queueKey = "it-offline-" + System.nanoTime();
        // 1s socket/connection timeouts: a BLPOP's bounded read timeout is socketTimeout x 12 plus
        // connectionTimeout (see BaseRedisConnection.buildClientConfig), i.e. ~65s at the 5s
        // defaults -- far too slow to observe a frozen Redis -- but ~13s here.
        OperationContext ctx = listenContext("List", queueKey, null, null, 60_000L, 5L, 1000L, 1L, 1L, 1L);
        RedisListenOperation op = new RedisListenOperation(ctx);

        List<String> received = new CopyOnWriteArrayList<>();
        Listener listener = mock(Listener.class);
        when(listener.isStopping()).thenReturn(false);
        org.mockito.Mockito.doAnswer(invocation -> {
            Payload payload = invocation.getArgument(0);
            received.add(readPayload(payload));
            return null;
        }).when(listener).submit(any(Payload.class));

        String containerId = REDIS.getContainerId();
        boolean paused = false;
        op.start(listener, new RedisListenManager());
        try {
            // Let the poll loop settle into its blocking BLPOP before the fault is injected.
            TimeUnit.MILLISECONDS.sleep(500);

            REDIS.getDockerClient().pauseContainerCmd(containerId).exec();
            paused = true;

            org.mockito.Mockito.verify(listener, org.mockito.Mockito.timeout(40_000L).atLeastOnce())
                    .updateStatus(org.mockito.ArgumentMatchers.eq(ListenerStatus.OFFLINE),
                            org.mockito.ArgumentMatchers.argThat(
                                    (String message) -> message != null && !message.trim().isEmpty()));

            REDIS.getDockerClient().unpauseContainerCmd(containerId).exec();
            paused = false;

            org.mockito.Mockito.verify(listener, org.mockito.Mockito.timeout(40_000L).atLeastOnce())
                    .updateStatus(ListenerStatus.ONLINE);

            try (Jedis direct = directClient()) {
                direct.rpush(queueKey, "after-recovery");
            }
            long deadline = System.currentTimeMillis() + 15_000L;
            while (received.isEmpty() && System.currentTimeMillis() < deadline) {
                TimeUnit.MILLISECONDS.sleep(100);
            }
        } finally {
            if (paused) {
                REDIS.getDockerClient().unpauseContainerCmd(containerId).exec();
            }
            op.stop();
        }

        assertFalse("Expected the recovered listener to deliver the message pushed after Redis returned",
                received.isEmpty());
        assertTrue("Expected the delivered payload to contain the pushed value; got: " + received.get(0),
                received.get(0).contains("after-recovery"));
    }
}
