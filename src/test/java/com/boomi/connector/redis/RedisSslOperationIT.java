package com.boomi.connector.redis;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.redis.pool.RedisClientPoolManager;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.SimpleOperationResult;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Proves the {@code useSSL} connection field end-to-end against a Redis serving <b>TLS only</b>
 * ({@code --port 0}), so the round trip can only succeed over an actual TLS handshake.
 *
 * <p>The self-signed server certificate is generated fresh on every run with the JDK's own
 * {@code keytool} (the redis:7 image ships no openssl CLI), exported to PEM in-process, and
 * copied into the container before Redis starts. Client trust is injected at runtime: the
 * generated cert is installed as the JVM-default {@link SSLContext} for the duration of this
 * class (restored in {@code @AfterClass}). The connector passes only {@code .ssl(true)} to
 * Jedis — no custom socket factory — so Jedis resolves the default context, exactly as on a
 * real Atom where the JVM truststore provides trust. No key material is committed to the repo.</p>
 */
@Category(IntegrationTest.class)
public class RedisSslOperationIT {

    private static final String ADMIN_PASS = "adminpass";
    private static final String BASIC_USER = "boomi-ssl";
    private static final String BASIC_PASS = "ssl-secret";
    private static final String KEY_PREFIX = "it:";
    private static final char[] STORE_PASS = "changeit".toCharArray();

    @SuppressWarnings("resource")
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7"))
                    .withExposedPorts(6379)
                    .withCommand("redis-server", "--port", "0", "--tls-port", "6379",
                            "--tls-cert-file", "/tls/redis.crt", "--tls-key-file", "/tls/redis.key",
                            "--tls-ca-cert-file", "/tls/ca.crt", "--tls-auth-clients", "no",
                            "--requirepass", ADMIN_PASS);

    private static String hostPort;
    private static SSLContext originalDefaultSslContext;

    @BeforeClass
    public static void startTlsRedisAndTrustItsCert() throws Exception {
        X509Certificate serverCert = generateSelfSignedCertIntoContainer();
        REDIS.start();
        hostPort = REDIS.getHost() + ":" + REDIS.getMappedPort(6379);

        // Named ACL user so Basic auth is exercised over TLS (redis-cli must itself speak TLS now).
        org.testcontainers.containers.Container.ExecResult r = REDIS.execInContainer(
                "redis-cli", "--tls", "--cacert", "/tls/ca.crt", "-a", ADMIN_PASS,
                "ACL", "SETUSER", BASIC_USER, "on", ">" + BASIC_PASS, "~*", "+@all");
        assertEquals("ACL SETUSER failed: " + r.getStderr(), 0, r.getExitCode());

        // Runtime trust injection: a trust store containing only the freshly-generated cert
        // becomes the JVM default. Jedis (via the connector's plain .ssl(true) config) resolves
        // SSLSocketFactory.getDefault() -> SSLContext.getDefault().
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("redis-test-cert", serverCert);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        SSLContext testContext = SSLContext.getInstance("TLS");
        testContext.init(null, tmf.getTrustManagers(), null);

        originalDefaultSslContext = SSLContext.getDefault();
        SSLContext.setDefault(testContext);
    }

    /**
     * Generates a throwaway self-signed cert (SAN localhost/127.0.0.1 to satisfy hostname checks
     * against the Testcontainers-mapped port) with the JDK's keytool, converts it to the PEM
     * files Redis expects, and stages them into the container image before startup.
     * The self-signed cert acts as its own CA.
     */
    private static X509Certificate generateSelfSignedCertIntoContainer() throws Exception {
        Path tmpDir = Files.createTempDirectory("redis-tls-it");
        Path p12 = tmpDir.resolve("redis.p12");
        String exe = System.getProperty("os.name").toLowerCase().contains("win") ? "keytool.exe" : "keytool";
        String keytool = Paths.get(System.getProperty("java.home"), "bin", exe).toString();

        Process proc = new ProcessBuilder(keytool, "-genkeypair",
                "-alias", "redis", "-keyalg", "RSA", "-keysize", "2048", "-validity", "2",
                "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1",
                "-storetype", "PKCS12", "-keystore", p12.toString(),
                "-storepass", new String(STORE_PASS))
                .redirectErrorStream(true).start();
        String output = new String(readAll(proc.getInputStream()), StandardCharsets.UTF_8);
        assertEquals("keytool failed: " + output, 0, proc.waitFor());

        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = new FileInputStream(p12.toFile())) {
            ks.load(in, STORE_PASS);
        }
        X509Certificate cert = (X509Certificate) ks.getCertificate("redis");
        PrivateKey key = (PrivateKey) ks.getKey("redis", STORE_PASS);

        String certPem = toPem("CERTIFICATE", cert.getEncoded());
        String keyPem = toPem("PRIVATE KEY", key.getEncoded()); // PKCS#8, which Redis/OpenSSL accepts

        // World-readable on purpose: files are staged root-owned, but the redis image's entrypoint
        // drops to the 'redis' user before starting the server — 0600 root would be unreadable and
        // Redis exits at startup. The key is throwaway, regenerated per run, never on the host disk.
        REDIS.withCopyToContainer(Transferable.of(certPem.getBytes(StandardCharsets.UTF_8)), "/tls/redis.crt");
        REDIS.withCopyToContainer(Transferable.of(keyPem.getBytes(StandardCharsets.UTF_8), 0644), "/tls/redis.key");
        REDIS.withCopyToContainer(Transferable.of(certPem.getBytes(StandardCharsets.UTF_8)), "/tls/ca.crt");

        Files.deleteIfExists(p12);
        Files.deleteIfExists(tmpDir);
        return cert;
    }

    private static String toPem(String type, byte[] der) {
        String base64 = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8)).encodeToString(der);
        return "-----BEGIN " + type + "-----\n" + base64 + "\n-----END " + type + "-----\n";
    }

    @AfterClass
    public static void restoreDefaultSslContextAndStopRedis() {
        if (originalDefaultSslContext != null) {
            SSLContext.setDefault(originalDefaultSslContext);
        }
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

    private static byte[] readAll(InputStream in) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int n;
        while ((n = in.read(buf)) != -1) {
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }

    private static Map<String, Object> connProps(boolean useSsl) {
        Map<String, Object> connProps = new HashMap<>();
        connProps.put("hosts", hostPort);
        connProps.put("useSSL", useSsl);
        connProps.put("authenticationType", "Basic");
        connProps.put("user", BASIC_USER);
        connProps.put("password", BASIC_PASS);
        connProps.put("poolEnabled", false);
        connProps.put("connectionTimeout", 5L);
        connProps.put("socketTimeout", 5L);
        return connProps;
    }

    private static Map<String, Object> opProps() {
        Map<String, Object> opProps = new HashMap<>();
        opProps.put("key_prefix", KEY_PREFIX);
        opProps.put("remove_key_prefix_from_response", true);
        opProps.put("throw_exception", true);
        return opProps;
    }

    @Test
    public void sslUpsertThenGetRoundTripsOverTls() throws Exception {
        RedisConnector connector = new RedisConnector();
        ConnectorTester tester = new ConnectorTester(connector);

        // UPSERT over TLS
        tester.setOperationContext(OperationType.UPSERT, connProps(true), opProps(), "Upsert", null);
        String upsertPayload = "{\"key\": \"ssl-key\", \"value\": \"TLS round trip value\"}";
        List<InputStream> upsertInputs = new ArrayList<>();
        upsertInputs.add(new ByteArrayInputStream(upsertPayload.getBytes(StandardCharsets.UTF_8)));
        List<SimpleOperationResult> upsertResults = tester.executeUpsertOperation(upsertInputs);

        assertEquals("expected exactly one upsert result", 1, upsertResults.size());
        assertEquals("upsert over TLS must succeed; message: " + upsertResults.get(0).getMessage(),
                OperationStatus.SUCCESS, upsertResults.get(0).getStatus());

        // GET over TLS
        tester.setOperationContext(OperationType.GET, connProps(true), opProps(), "Get", null);
        List<SimpleOperationResult> getResults = tester.executeGetOperation("ssl-key");

        assertEquals("expected exactly one get result", 1, getResults.size());
        SimpleOperationResult getResult = getResults.get(0);
        assertEquals("get over TLS must succeed; message: " + getResult.getMessage(),
                OperationStatus.SUCCESS, getResult.getStatus());
        assertFalse("get must return a payload", getResult.getPayloads().isEmpty());

        String json = new String(getResult.getPayloads().get(0), StandardCharsets.UTF_8);
        assertTrue("payload must contain the upserted value; got: " + json,
                json.contains("TLS round trip value"));
    }

    @Test
    public void plaintextClientCannotReachTheTlsOnlyServer() throws Exception {
        // Negative control: with useSSL=false against a TLS-only Redis the operation must fail,
        // proving the useSSL flag genuinely toggles TLS rather than being ignored.
        RedisConnector connector = new RedisConnector();
        ConnectorTester tester = new ConnectorTester(connector);

        tester.setOperationContext(OperationType.GET, connProps(false), opProps(), "Get", null);
        List<SimpleOperationResult> results = tester.executeGetOperation("ssl-key");

        assertEquals("expected exactly one result", 1, results.size());
        assertEquals("plaintext GET against a TLS-only server must fail",
                OperationStatus.FAILURE, results.get(0).getStatus());
    }
}
