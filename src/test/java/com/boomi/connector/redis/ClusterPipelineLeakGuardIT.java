package com.boomi.connector.redis;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.redis.pool.RedisClientPoolManager;
import com.boomi.connector.testutil.SimpleBrowseContext;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Regression coverage for the ClusterPipeline connection leak: {@code MultiNodePipelineBase.sync()}
 * only drains responses - only {@code close()} returns the per-node connections a pipeline
 * borrowed from the cluster's internal pools (verified against the Jedis 5.2.0 sources). A
 * sync-without-close leaks one pooled connection per touched node on every wildcard GET/DELETE,
 * which permanently exhausts the shared cluster client's per-node pool after a handful of
 * executions. Mockito-based unit tests cannot catch this - nothing in them models real pool
 * exhaustion - so this runs the real thing against a live single-node Redis Cluster.
 */
@Category(IntegrationTest.class)
public class ClusterPipelineLeakGuardIT {

    /** Chosen to be unusual enough to avoid colliding with a developer's other local services. */
    private static final int DATA_PORT = 17179;

    /** Testcontainers has no public fixed-host-port API on GenericContainer; this exposes it. */
    private static class FixedPortContainer extends GenericContainer<FixedPortContainer> {
        FixedPortContainer(DockerImageName image) {
            super(image);
        }

        FixedPortContainer withFixedPort(int hostPort, int containerPort) {
            super.addFixedExposedPort(hostPort, containerPort);
            return this;
        }
    }

    @SuppressWarnings("resource")
    private static final FixedPortContainer REDIS = new FixedPortContainer(DockerImageName.parse("redis:7"))
            .withFixedPort(DATA_PORT, 6379)
            .withCommand("redis-server",
                    "--cluster-enabled", "yes",
                    "--cluster-config-file", "nodes.conf",
                    "--cluster-node-timeout", "5000",
                    // A real (non-Testcontainers-mapped) cluster node advertises its own container-
                    // internal address, which the test JVM on the host could never reach. Announcing
                    // the host-reachable fixed port instead is what makes CLUSTER SLOTS resolvable
                    // from outside the container. Nothing else ever needs to reach the bus port
                    // (single-node "cluster", no peers to gossip with) so it is never exposed.
                    "--cluster-announce-ip", "127.0.0.1",
                    "--cluster-announce-port", String.valueOf(DATA_PORT),
                    "--cluster-announce-bus-port", String.valueOf(DATA_PORT + 10000));

    @BeforeClass
    public static void startCluster() throws Exception {
        REDIS.start();
        exec("redis-cli", "-p", "6379", "CLUSTER", "ADDSLOTSRANGE", "0", "16383");
        waitForClusterOk();
    }

    @AfterClass
    public static void stopCluster() {
        REDIS.stop();
    }

    @Before
    public void resetSharedClustersBefore() {
        RedisClientPoolManager.closeAll();
    }

    @After
    public void resetSharedClustersAfter() {
        RedisClientPoolManager.closeAll();
    }

    private static Container.ExecResult exec(String... cmd) throws Exception {
        Container.ExecResult r = REDIS.execInContainer(cmd);
        assertEquals("cmd failed: " + String.join(" ", cmd) + " -> " + r.getStderr(), 0, r.getExitCode());
        return r;
    }

    private static void waitForClusterOk() throws Exception {
        for (int i = 0; i < 20; i++) {
            Container.ExecResult r = exec("redis-cli", "-p", "6379", "CLUSTER", "INFO");
            if (r.getStdout().contains("cluster_state:ok")) {
                return;
            }
            Thread.sleep(250);
        }
        fail("Redis Cluster did not reach cluster_state:ok in time");
    }

    private static int connectedClients() throws Exception {
        Container.ExecResult r = exec("redis-cli", "-p", "6379", "INFO", "clients");
        for (String line : r.getStdout().split("\r?\n")) {
            if (line.startsWith("connected_clients:")) {
                return Integer.parseInt(line.substring("connected_clients:".length()).trim());
            }
        }
        throw new IllegalStateException("connected_clients not found in INFO clients output: " + r.getStdout());
    }

    private static RedisConnection sharedClusterConnection(int poolSize) {
        Map<String, Object> conn = new HashMap<>();
        conn.put("hosts", "127.0.0.1:" + DATA_PORT);
        conn.put("useSSL", false);
        conn.put("authenticationType", "None");
        conn.put("clusteringPolicy", "OSSClustered");
        conn.put("poolEnabled", true);
        conn.put("poolSize", (long) poolSize);
        conn.put("minPoolSize", 1L);
        conn.put("connectionTimeout", 5L);
        conn.put("socketTimeout", 5L);
        conn.put("maxWaitTime", 2L);
        SimpleBrowseContext bc = new SimpleBrowseContext(null, null, OperationType.GET, conn, new HashMap<>());
        return new RedisConnection(bc);
    }

    @Test
    public void wildcardGetAndDeleteDoNotLeakClusterPipelineConnections() throws Exception {
        int poolSize = 2;
        // Comfortably more than a pool of this size could ever cover if each iteration leaked a
        // connection - the unfixed code would exhaust the pool and start blocking/throwing well
        // before this many iterations complete.
        int iterations = poolSize * 6;

        int before = connectedClients();

        for (int i = 0; i < iterations; i++) {
            RedisConnection conn = sharedClusterConnection(poolSize);
            try {
                conn.set("leak-guard:" + i, "v", -1L);
                Map<String, String> all = conn.getAll("leak-guard:*"); // exercises ClusterPipeline.get
                assertNotNull(all);
                conn.delAll("leak-guard:");                            // exercises ClusterPipeline.del
            } finally {
                conn.close();
            }
        }

        int after = connectedClients();
        assertTrue("connected_clients should stay bounded near the configured pool size (" + poolSize
                        + "); before=" + before + " after=" + after + " - a growing count means the "
                        + "pipeline is leaking borrowed connections back to Redis",
                after <= before + poolSize + 2);

        // Functional check in addition to the server-side count: with the leak fixed, the shared
        // pool never runs dry, so one more execution must complete immediately (well under the
        // configured 2s maxWaitTime). With the leak present, this either blocks for maxWaitTime or
        // throws once the pool's connections are exhausted.
        long start = System.currentTimeMillis();
        RedisConnection probe = sharedClusterConnection(poolSize);
        try {
            probe.set("leak-guard:probe", "v", -1L);
            assertEquals("v", probe.get("leak-guard:probe"));
        } finally {
            probe.close();
        }
        long elapsedMs = System.currentTimeMillis() - start;
        assertTrue("expected an immediately available pool connection, took " + elapsedMs + "ms",
                elapsedMs < 1000);
    }
}
