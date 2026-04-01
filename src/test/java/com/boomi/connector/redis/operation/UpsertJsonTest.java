package com.boomi.connector.redis.operation;

import com.boomi.connector.redis.util.RedisUtils;
import com.google.gson.JsonObject;
import org.junit.Test;

import static org.junit.Assert.*;

public class UpsertJsonTest {

    @Test
    public void testParseValidJson() throws Exception {
        String json = "{\"key\": \"user:123\", \"value\": \"John Doe\"}";
        JsonObject obj = RedisUtils.parseJson(json);
        assertEquals("user:123", RedisUtils.getJsonStringValue(obj, "key"));
        assertEquals("John Doe", RedisUtils.getJsonStringValue(obj, "value"));
    }

    @Test
    public void testParseJsonWithNullValue() throws Exception {
        String json = "{\"key\": \"user:456\", \"value\": null}";
        JsonObject obj = RedisUtils.parseJson(json);
        assertEquals("user:456", RedisUtils.getJsonStringValue(obj, "key"));
        assertNull(RedisUtils.getJsonStringValue(obj, "value"));
    }

    @Test
    public void testParseJsonWithMissingField() throws Exception {
        String json = "{\"key\": \"user:789\"}";
        JsonObject obj = RedisUtils.parseJson(json);
        assertEquals("user:789", RedisUtils.getJsonStringValue(obj, "key"));
        assertNull(RedisUtils.getJsonStringValue(obj, "value"));
    }

    @Test(expected = Exception.class)
    public void testParseInvalidJsonThrows() throws Exception {
        RedisUtils.parseJson("not-valid-json");
    }
}
