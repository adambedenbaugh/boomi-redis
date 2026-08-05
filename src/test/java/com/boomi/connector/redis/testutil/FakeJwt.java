package com.boomi.connector.redis.testutil;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** Builds an unsigned JWT whose payload carries an 'oid' claim, matching the connector's
 *  Base64.getUrlDecoder() (base64url, no padding). For local tests only. */
public final class FakeJwt {
    private FakeJwt() { }

    public static String token(String oid) {
        return token(oid, (System.currentTimeMillis() / 1000L) + 300L);
    }

    public static String token(String oid, long expEpochSeconds) {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String header = enc.encodeToString("{\"alg\":\"none\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
        String payload = enc.encodeToString(
                ("{\"oid\":\"" + oid + "\",\"exp\":" + expEpochSeconds + "}").getBytes(StandardCharsets.UTF_8));
        return header + "." + payload + ".";
    }
}
