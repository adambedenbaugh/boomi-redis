package com.boomi.connector.redis.util;

import com.google.gson.JsonObject;
import org.junit.Test;

import static org.junit.Assert.*;

public class JsonTest {

    @Test
    public void testParseJsonAndGetValues() throws Exception {
        String json = "{ \"key\": \"test123\", \"value\": \"test value\" }";
        JsonObject obj = RedisUtils.parseJson(json);
        assertEquals("test123", RedisUtils.getJsonStringValue(obj, "key"));
        assertEquals("test value", RedisUtils.getJsonStringValue(obj, "value"));
    }

    @Test
    public void testGetJsonStringValueReturnsNullForMissingField() throws Exception {
        String json = "{ \"key\": \"test123\" }";
        JsonObject obj = RedisUtils.parseJson(json);
        assertNull(RedisUtils.getJsonStringValue(obj, "missing"));
    }

    @Test
    public void testRemovePrefixStripsLeadingPrefix() {
        assertEquals("key", RedisUtils.removePrefix("prefix:key", "prefix:"));
        assertEquals("key", RedisUtils.removePrefix("key", ""));
    }

    @Test
    public void testRemovePrefixNoMatchReturnsOriginal() {
        assertEquals("other:key", RedisUtils.removePrefix("other:key", "prefix:"));
    }

    @Test
    public void testRemovePrefixWithNullInputs() {
        assertNull(RedisUtils.removePrefix(null, "prefix:"));
        assertEquals("key", RedisUtils.removePrefix("key", null));
    }
}
