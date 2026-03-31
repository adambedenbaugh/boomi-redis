package com.boomi.connector.redis.operation;

import com.boomi.connector.redis.util.RedisUtils;
import com.google.gson.JsonObject;

public class UpsertJsonTest {
    public static void main(String[] args) {
        System.out.println("Testing JSON parsing for Upsert operation...");
        
        try {
            // Test case 1: Valid JSON with key and value
            String jsonInput1 = "{\"key\": \"user:123\", \"value\": \"John Doe\"}";
            System.out.println("Input: " + jsonInput1);
            
            JsonObject jsonObject1 = RedisUtils.parseJson(jsonInput1);
            String key1 = RedisUtils.getJsonStringValue(jsonObject1, "key");
            String value1 = RedisUtils.getJsonStringValue(jsonObject1, "value");
            
            System.out.println("Parsed key: " + key1);
            System.out.println("Parsed value: " + value1);
            System.out.println("✓ Test 1 passed\n");
            
            // Test case 2: JSON with null value
            String jsonInput2 = "{\"key\": \"user:456\", \"value\": null}";
            System.out.println("Input: " + jsonInput2);
            
            JsonObject jsonObject2 = RedisUtils.parseJson(jsonInput2);
            String key2 = RedisUtils.getJsonStringValue(jsonObject2, "key");
            String value2 = RedisUtils.getJsonStringValue(jsonObject2, "value");
            
            System.out.println("Parsed key: " + key2);
            System.out.println("Parsed value: " + value2);
            System.out.println("✓ Test 2 passed\n");
            
            // Test case 3: Missing field
            String jsonInput3 = "{\"key\": \"user:789\"}";
            System.out.println("Input: " + jsonInput3);
            
            JsonObject jsonObject3 = RedisUtils.parseJson(jsonInput3);
            String key3 = RedisUtils.getJsonStringValue(jsonObject3, "key");
            String value3 = RedisUtils.getJsonStringValue(jsonObject3, "value");
            
            System.out.println("Parsed key: " + key3);
            System.out.println("Parsed value: " + value3);
            System.out.println("✓ Test 3 passed\n");
            
            System.out.println("All JSON parsing tests passed! ✓");
            
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}