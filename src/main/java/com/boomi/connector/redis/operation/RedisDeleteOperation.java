package com.boomi.connector.redis.operation;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DeleteRequest;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.redis.RedisConnection;
import com.boomi.connector.util.BaseDeleteOperation;


public class RedisDeleteOperation extends BaseDeleteOperation {

	Logger logger;

	public RedisDeleteOperation(OperationContext context) {
		super(context);
	}

	@Override
	protected void executeDelete(DeleteRequest request, OperationResponse response) {
		logger = response.getLogger();
		
		String keyPrefix = getContext().getOperationProperties().getProperty("key_prefix", "");
		// boolean autoKey = getContext().getOperationProperties().getBooleanProperty("auto_key");

		RedisConnection redisConnection = new RedisConnection(getContext());

		try {
			redisConnection.init();

			for (ObjectIdData input : request) {
				try {
					String objectId = input.getObjectId();
					if (objectId == null || objectId.isEmpty()) {
						throw new ConnectorException("The DELETE operation received an empty id (key). "
								+ "Provide a key, or '*' to delete all keys with the configured prefix.");
					}
					if ("*".equals(objectId)) {
						logger.fine("Found wildcard objectId - executing batch delete");
						delete(redisConnection, keyPrefix);
					} else {
						delete(redisConnection, keyPrefix, objectId);
					}
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

	public void delete(RedisConnection redisConnection, String keyPrefix) {
		// Pass the raw prefix: the connection layer's prepareScanPattern is the single point that
		// escapes glob metacharacters and appends the trailing wildcard, so wildcard DELETE and
		// wildcard GET share identical matching semantics.
		redisConnection.delAll(keyPrefix);
	}

	public void delete(RedisConnection redisConnection, String keyPrefix, String key) {
		String combinedKey = keyPrefix + key;
		logger.fine("Deleting key: " + combinedKey);
		redisConnection.del(combinedKey);
	}
}