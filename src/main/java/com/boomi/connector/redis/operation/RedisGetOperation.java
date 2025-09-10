package com.boomi.connector.redis.operation;

import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import com.boomi.connector.api.GetRequest;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.redis.RedisConnection;
import com.boomi.connector.util.BaseGetOperation;


/**
 * 
 * @author anthony.rabiaza@gmail.com
 *
 */
public class RedisGetOperation extends BaseGetOperation {

	private static final String WILDCARD_OBJECT_ID = "*";
	private static final String SUCCESS_STATUS_CODE = "200";
	private static final String SUCCESS_MESSAGE = "OK";

	// Configuration class to hold operation properties
	private class RedisOperationConfig {
		private final String keyPrefix;
		private final boolean autoKey;
		private final boolean throwException;
		private final boolean wrapInProfile;
		private final long ttl;
		private final Logger logger;

		public RedisOperationConfig(OperationContext context, Logger logger) {
			this.keyPrefix = context.getOperationProperties().getProperty("key_prefix");
			this.autoKey = context.getOperationProperties().getBooleanProperty("auto_key");
			this.throwException = context.getOperationProperties().getBooleanProperty("throw_exception");
			this.wrapInProfile = context.getOperationProperties().getBooleanProperty("wrap_inprofile");
			this.logger = logger;
			
			long ttlValue;
			try {
				ttlValue = context.getOperationProperties().getLongProperty("set_ttl");
			} catch (Exception e) {
				ttlValue = -1;
			}
			this.ttl = ttlValue;
		
		}

		public String getKeyPrefix() { return keyPrefix; }
		public boolean isAutoKey() { return autoKey; }
		public boolean isThrowException() { return throwException; }
		public boolean isWrapInProfile() { return wrapInProfile; }
		public long getTtl() { return ttl; }
		public Logger getLogger() { return logger; }
	}

	public RedisGetOperation(OperationContext context) {
		super(context);
	}

	@Override
	protected void executeGet(GetRequest request, OperationResponse response) {
		Logger logger = response.getLogger();
		logger.fine("executeGet");

		RedisOperationConfig config = new RedisOperationConfig(getContext(), logger);
		ObjectIdData input = request.getObjectId();

		try {
			String objectId = input.getObjectId();
			logger.fine("ID: " + objectId);

			RedisConnection redisConnection = new RedisConnection(getContext());

			if (!WILDCARD_OBJECT_ID.equals(objectId)) {
				handleSingleGet(objectId, config, redisConnection, response, input);
			} else {
				handleGetAll(config, redisConnection, response, input);
			}

		} catch (Exception e) {
			ResponseUtil.addExceptionFailure(response, input, e);
		}
	}

	private void handleSingleGet(String objectId, RedisOperationConfig config, 
	                           RedisConnection connection, OperationResponse response, 
	                           ObjectIdData input) {
		String cachedValue = get(connection, config.getKeyPrefix(), objectId, config.getTtl());

		if (config.isThrowException() && cachedValue == null) {
			ResponseUtil.addExceptionFailure(response, input, new Exception("Value not found in the Cache"));
			return;
		}

		config.getLogger().fine("CacheValue received:" + cachedValue);

		if (cachedValue != null) {
			config.getLogger().fine("Cache Hit");
			if (config.isWrapInProfile()) {
				response.addResult(input, OperationStatus.SUCCESS, SUCCESS_STATUS_CODE, SUCCESS_MESSAGE, 
					ResponseUtil.toPayload("<Get><ID>" + objectId + "</ID><Value>" + cachedValue + "</Value></Get>"));
			} else {
				response.addResult(input, OperationStatus.SUCCESS, SUCCESS_STATUS_CODE, SUCCESS_MESSAGE, 
					ResponseUtil.toPayload(cachedValue));
			}
		} else {
			config.getLogger().fine("Cache Empty");
			response.addEmptyResult(input, OperationStatus.SUCCESS, SUCCESS_STATUS_CODE, SUCCESS_MESSAGE);
		}
	}

	private void handleGetAll(RedisOperationConfig config, RedisConnection connection, 
	                        OperationResponse response, ObjectIdData input) {
		Map<String, String> cachedValue = get(connection, config.getKeyPrefix(), config.getTtl());
		
		if (config.isThrowException() && cachedValue == null) {
			ResponseUtil.addExceptionFailure(response, input, new Exception("Value not found in the Cache"));
			return;
		}

		config.getLogger().fine("CacheValue received: " + cachedValue);

		if (cachedValue != null) {
			config.getLogger().fine("Cache Hit");
			for (Iterator<String> iterator = cachedValue.keySet().iterator(); iterator.hasNext();) {
				String key = iterator.next();
				String value = cachedValue.get(key);

				if (config.isWrapInProfile()) {
					response.addPartialResult(input, OperationStatus.SUCCESS, SUCCESS_STATUS_CODE, SUCCESS_MESSAGE, 
						ResponseUtil.toPayload("<Get><ID>" + key + "</ID><Value>" + value + "</Value></Get>"));
				} else {
					response.addPartialResult(input, OperationStatus.SUCCESS, SUCCESS_STATUS_CODE, SUCCESS_MESSAGE, 
						ResponseUtil.toPayload(value));
				}
			}
			
			response.finishPartialResult(input);
		} else {
			config.getLogger().fine("Cache Empty");
			response.addEmptyResult(input, OperationStatus.SUCCESS, SUCCESS_STATUS_CODE, SUCCESS_MESSAGE);
		}
	}

	// Convenience methods for the connector operations
	public Map<String, String> get(RedisConnection redisConnection, String keyPrefix, Long ttl) {
		return redisConnection.getAll(keyPrefix, ttl);
	}
	
	public String get(RedisConnection redisConnection, String keyPrefix, String key, Long ttl) {
		String combinedKey = keyPrefix + ":" + key;
		return redisConnection.getValue(combinedKey, ttl);
	}
}