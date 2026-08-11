package com.boomi.connector.redis.operation;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
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
		
		String keyPrefix = getContext().getOperationProperties().getProperty("key_prefix", "");
		// set_ttl is overrideable="true", so its effective value can be supplied per-document as a
		// Dynamic Operation Property. Overridable String/Integer/Boolean properties must be read from
		// ObjectData.getDynamicOperationProperties() (per the SDK contract), not getOperationProperties().
		// The static value is the fallback when no override is present.
		long staticTtl = getContext().getOperationProperties().getLongProperty("set_ttl", -1L);

		RedisConnection redisConnection = new RedisConnection(getContext());

		try {
			redisConnection.init();

			for (ObjectData input : request) {
				try {
					long ttl = resolveTtl(input, staticTtl);
					String inputStr = RedisUtils.inputStreamToString(input.getData());
					JsonObject jsonObject = RedisUtils.parseJson(inputStr);
					String objectId = RedisUtils.getJsonStringValue(jsonObject, "key");
					String value = RedisUtils.getJsonStringValue(jsonObject, "value");

					if (objectId == null || objectId.isEmpty()) {
						throw new ConnectorException("The document is missing a non-empty \"key\" field.");
					}
					if (value == null) {
						throw new ConnectorException("The document is missing a \"value\" field.");
					}

					upsert(redisConnection, keyPrefix, objectId, value, ttl);
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

	/**
	 * Resolves the effective TTL for a single document. A Dynamic Operation Property override for
	 * {@code set_ttl} arrives via {@link ObjectData#getDynamicOperationProperties()}; when absent, the
	 * statically-configured operation value is used.
	 */
	long resolveTtl(ObjectData input, long staticTtl) {
		String override = input.getDynamicOperationProperties().getProperty("set_ttl");
		if (override == null || override.trim().isEmpty()) {
			return staticTtl;
		}
		try {
			return Long.parseLong(override.trim());
		} catch (NumberFormatException e) {
			logger.log(Level.SEVERE, "Invalid set_ttl override: " + override, e);
			throw new ConnectorException("Invalid TTL override '" + override
					+ "' for set_ttl; expected an integer number of milliseconds (-1 to leave TTL unchanged).", e);
		}
	}

	public void upsert(RedisConnection redisConnection, String keyPrefix, String key, String value, Long ttl) {
		String combinedKey = keyPrefix  + key;
		logger.fine("Upserting key: " + combinedKey + " with TTL: " + ttl);
		redisConnection.set(combinedKey, value, ttl);
	}
}