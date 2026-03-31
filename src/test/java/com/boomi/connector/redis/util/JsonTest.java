package com.boomi.connector.redis.util;

import com.google.gson.JsonObject;

public class JsonTest {
    public static void main(String[] args) {
        try {
            // Test JSON parsing
            String jsonString = "{ \"key\": \"test123\", \"value\": \"test value\" }";
            
            JsonObject jsonObject = RedisUtils.parseJson(jsonString);
            String key = RedisUtils.getJsonStringValue(jsonObject, "key");
            String value = RedisUtils.getJsonStringValue(jsonObject, "value");
            
            System.out.println("Successfully parsed JSON:");
            System.out.println("Key: " + key);
            System.out.println("Value: " + value);
            
            // Test with missing field
            String missingField = RedisUtils.getJsonStringValue(jsonObject, "missing");
            System.out.println("Missing field result: " + missingField);
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}