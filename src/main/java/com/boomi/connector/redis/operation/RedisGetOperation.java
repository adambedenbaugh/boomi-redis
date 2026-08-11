package com.boomi.connector.redis.operation;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.logging.Logger;

import com.boomi.connector.api.GetRequest;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.redis.RedisConnection;
import com.boomi.connector.redis.util.RedisUtils;
import com.boomi.connector.util.BaseGetOperation;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class RedisGetOperation extends BaseGetOperation {

	private static final String WILDCARD_OBJECT_ID = "*";
	private static final String SUCCESS_STATUS_CODE = "200";
	private static final String SUCCESS_MESSAGE = "OK";

	private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

	// JSON serialization for the response
	private static class KeyValuePair {
		@SuppressWarnings("unused")
		private final String key;
		@SuppressWarnings("unused")
		private final String value;

		public KeyValuePair(String key, String value) {
			this.key = key;
			this.value = value;
		}
	}

	public RedisGetOperation(OperationContext context) {
		super(context);
	}

	@Override
	protected void executeGet(GetRequest request, OperationResponse response) {
		Logger logger = response.getLogger();

		String keyPrefix = getContext().getOperationProperties().getProperty("key_prefix", "");
		boolean removeKeyPrefixFromResponse = getContext().getOperationProperties()
				.getBooleanProperty("remove_key_prefix_from_response", true);
		boolean throwException = getContext().getOperationProperties()
				.getBooleanProperty("throw_exception", false);

		ObjectIdData input = request.getObjectId();
		RedisConnection redisConnection = new RedisConnection(getContext());

		try {
			redisConnection.init();
			String objectId = input.getObjectId();

			if (!WILDCARD_OBJECT_ID.equals(objectId)) {
				handleSingleGet(objectId, redisConnection, response, input, logger,
						keyPrefix, removeKeyPrefixFromResponse, throwException);
			} else {
				handleGetAll(redisConnection, response, input, logger,
						keyPrefix, removeKeyPrefixFromResponse, throwException);
			}

		} catch (Exception e) {
			ResponseUtil.addExceptionFailure(response, input, e);
		} finally {
			redisConnection.close();
		}
	}

	private void handleSingleGet(String objectId, RedisConnection connection, OperationResponse response,
	                           ObjectIdData input, Logger logger, String keyPrefix,
	                           boolean removeKeyPrefixFromResponse, boolean throwException) {
		String combinedKey = keyPrefix + objectId;
		logger.fine("Key: " + combinedKey);
		String cachedValue = connection.get(combinedKey);

		if (throwException && cachedValue == null) {
			ResponseUtil.addExceptionFailure(response, input, new Exception("Key not found."));
			return;
		}

		if (cachedValue != null) {
			List<KeyValuePair> keyValueList = new ArrayList<>();
			if (removeKeyPrefixFromResponse) {
				String keyWithoutPrefix = RedisUtils.removePrefix(combinedKey, keyPrefix);
				keyValueList.add(new KeyValuePair(keyWithoutPrefix, cachedValue));
			} else {
				keyValueList.add(new KeyValuePair(combinedKey, cachedValue));
			}
			String jsonResponse = gson.toJson(keyValueList);
			response.addResult(input, OperationStatus.SUCCESS, SUCCESS_STATUS_CODE, SUCCESS_MESSAGE,
				ResponseUtil.toPayload(jsonResponse));
		} else {
			logger.fine("Key not found.");
			response.addEmptyResult(input, OperationStatus.SUCCESS, SUCCESS_STATUS_CODE, SUCCESS_MESSAGE);
		}
	}

	private void handleGetAll(RedisConnection connection, OperationResponse response, ObjectIdData input,
	                        Logger logger, String keyPrefix, boolean removeKeyPrefixFromResponse,
	                        boolean throwException) {
		logger.fine("Get all keys with prefix: " + keyPrefix);
		Map<String, String> cachedValue = connection.getAll(keyPrefix);

		if (throwException && cachedValue.isEmpty()) {
			ResponseUtil.addExceptionFailure(response, input, new Exception("No keys found matching the configured prefix."));
			return;
		}

		if (!cachedValue.isEmpty()) {
			List<KeyValuePair> keyValueList = new ArrayList<>();
			for (Map.Entry<String, String> entry : cachedValue.entrySet()) {
				if (removeKeyPrefixFromResponse) {
					String fullKey = entry.getKey();
					String keyWithoutPrefix = RedisUtils.removePrefix(fullKey, keyPrefix);
					keyValueList.add(new KeyValuePair(keyWithoutPrefix, entry.getValue()));
				} else {
					keyValueList.add(new KeyValuePair(entry.getKey(), entry.getValue()));
				}
			}
			String jsonResponse = gson.toJson(keyValueList);
			response.addResult(input, OperationStatus.SUCCESS, SUCCESS_STATUS_CODE, SUCCESS_MESSAGE,
				ResponseUtil.toPayload(jsonResponse));
		} else {
			response.addEmptyResult(input, OperationStatus.SUCCESS, SUCCESS_STATUS_CODE, SUCCESS_MESSAGE);
		}
	}

}
