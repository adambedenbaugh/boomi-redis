package com.boomi.connector.redis.operation;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.redis.RedisConnection;
import com.boomi.connector.redis.util.RedisUtils;
import com.boomi.connector.util.BaseUpdateOperation;

import com.google.gson.JsonObject;

public class RedisUpsertOperation extends BaseUpdateOperation {
	
	Logger logger;

	public RedisUpsertOperation(OperationContext context) {
		super(context);
	}

	@Override
	protected void executeUpdate(UpdateRequest request, OperationResponse response) {
		logger = response.getLogger();
		
		String keyPrefix = getContext().getOperationProperties().getProperty("key_prefix");
		long ttl = getContext().getOperationProperties().getLongProperty("set_ttl", -1L);
		
		RedisConnection redisConnection = new RedisConnection(getContext());
		redisConnection.init();
		
		for (ObjectData input : request) {
			try {
				String inputStr = RedisUtils.inputStreamToString(input.getData());
				JsonObject jsonObject = RedisUtils.parseJson(inputStr);
				String objectId = RedisUtils.getJsonStringValue(jsonObject, "key");
				String value = RedisUtils.getJsonStringValue(jsonObject, "value");

				upsert(redisConnection, keyPrefix, objectId, value, ttl);
				response.addEmptyResult(input, OperationStatus.SUCCESS, "200", "OK");
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Details of Exception:", e);
				ResponseUtil.addExceptionFailure(response, input, e);
			}
		}
	}

	public void upsert(RedisConnection redisConnection, String keyPrefix, String key, String value, Long ttl) {
		String combinedKey = keyPrefix  + key;
		logger.fine("Upserting key: " + combinedKey + " with value: " + value + " and TTL: " + ttl);
		redisConnection.set(combinedKey, value, ttl);
	}
}