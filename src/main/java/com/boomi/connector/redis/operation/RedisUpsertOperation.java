package com.boomi.connector.redis.operation;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.w3c.dom.Document;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.redis.RedisConnection;
import com.boomi.connector.redis.util.RedisUtils;
import com.boomi.connector.util.BaseUpdateOperation;

/**
 * 
 * @author anthony.rabiaza@gmail.com
 *
 */
public class RedisUpsertOperation extends BaseUpdateOperation {

	public RedisUpsertOperation(OperationContext context) {
		super(context);
	}

	@Override
	protected void executeUpdate(UpdateRequest request, OperationResponse response) {
		Logger logger = response.getLogger();
		logger.fine("executeUpdate");
		
		String keyPrefix = getContext().getOperationProperties().getProperty("key_prefix");
		logger.fine("KeyPrefix: " + keyPrefix);
		
		boolean autoKey = getContext().getOperationProperties().getBooleanProperty("auto_key");
		logger.fine("AutoKey: " + autoKey);

		long ttl;
		try {
			ttl = getContext().getOperationProperties().getLongProperty("set_ttl");
		} catch (Exception e) {
			ttl = -1;
		}
		
		RedisConnection redisConnection = new RedisConnection(getContext());
		
		int i=0;
		for (ObjectData input : request) {
			logger.fine("Processing input " + i++);
			try {
				String inputStr = RedisUtils.inputStreamToString(input.getData());
				Document doc = RedisUtils.parse(RedisUtils.stringToInputStream(inputStr));
				String objectId = RedisUtils.getFirstNodeTextContent(doc, "//Upsert/ID");

				String value = RedisUtils.getFirstNodeTextContent(doc, "//Upsert/Value");
				upsert(redisConnection, keyPrefix, objectId, value, ttl);

				response.addResult(input, OperationStatus.SUCCESS, "200", "OK", ResponseUtil.toPayload(RedisUtils.stringToInputStream(value)));
			} catch (Exception e) {
				// make best effort to process every input
				logger.log(Level.SEVERE, "Details of Exception:", e);
				ResponseUtil.addExceptionFailure(response, input, e);
			}
		}
		logger.fine("End of processing");
	}

	public void upsert(RedisConnection redisConnection, String keyPrefix, String key, String value, Long ttl) {
		String combinedKey = keyPrefix + ":" + key;
		redisConnection.set(combinedKey, value, ttl);
	}
}