package com.boomi.connector.redis.operation;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.connector.util.ResponseUtil;
import com.boomi.connector.redis.RedisConnection;
import com.boomi.connector.redis.util.RedisUtils;

import com.google.gson.JsonObject;

public class RedisSendOperation extends BaseUpdateOperation {

	Logger logger;

	public RedisSendOperation(OperationContext context) {
		super(context);
	}

	@Override
	protected void executeUpdate(UpdateRequest request, OperationResponse response) {
		logger = response.getLogger();

		String queueType = getContext().getOperationProperties().getProperty("queue_type", "List");
		RedisConnection redisConnection = new RedisConnection(getContext());

		try {
			redisConnection.init();

			for (ObjectData input : request) {
				try {
					String inputStr = RedisUtils.inputStreamToString(input.getData());
					JsonObject jsonObject = RedisUtils.parseJson(inputStr);
					String key = RedisUtils.getJsonStringValue(jsonObject, "key");
					String value = RedisUtils.getJsonStringValue(jsonObject, "value");

					if (key == null || key.isEmpty()) {
						throw new ConnectorException("The document is missing a non-empty \"key\" field.");
					}
					if (value == null) {
						throw new ConnectorException("The document is missing a \"value\" field.");
					}

					send(redisConnection, queueType, key, value);
					response.addEmptyResult(input, OperationStatus.SUCCESS, "200", "OK");
				} catch (Exception e) {
					logger.log(Level.SEVERE, "Details of Exception:", e);
					ResponseUtil.addExceptionFailure(response, input, e);
				}
			}
		} finally {
			redisConnection.close();
		}
	}

	private void send(RedisConnection redisConnection, String queueType, String key, String value) {
		if ("Stream".equals(queueType)) {
			logger.fine("Adding to stream: " + key);
			redisConnection.xadd(key, value);
		} else {
			logger.fine("Pushing to list: " + key);
			redisConnection.rpush(key, value);
		}
	}
}
