package com.boomi.connector.redis.testutil;

import org.junit.Test;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import static org.junit.Assert.assertTrue;

public class FakeJwtTest {
    @Test
    public void payloadDecodesWithStandardDecoderAndContainsOid() {
        String token = FakeJwt.token("my-oid");
        String payload = token.split("\\.")[1];
        String json = new String(Base64.getUrlDecoder().decode(payload), StandardCharsets.UTF_8);
        assertTrue(json.contains("\"oid\":\"my-oid\""));
    }
}
