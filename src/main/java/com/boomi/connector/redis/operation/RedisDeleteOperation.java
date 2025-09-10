package com.boomi.connector.redis.operation;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.DeleteRequest;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.redis.RedisConnection;
import com.boomi.connector.util.BaseDeleteOperation;

/**
 * 
 * @author anthony.rabiaza@gmail.com
 *
 */
public class RedisDeleteOperation extends BaseDeleteOperation {

	public RedisDeleteOperation(OperationContext context) {
		super(context);
	}

	@Override
	protected void executeDelete(DeleteRequest request, OperationResponse response) {
		Logger logger = response.getLogger();
		logger.fine("executeDelete");
		
		String keyPrefix = getContext().getOperationProperties().getProperty("key_prefix");
		logger.fine("KeyPrefix: " + keyPrefix);
		
		boolean autoKey = getContext().getOperationProperties().getBooleanProperty("auto_key");
		logger.fine("AutoKey: " + autoKey);
		
		RedisConnection redisConnection = new RedisConnection(getContext());
		
		int i=0;
		for (ObjectIdData input : request) {
			logger.fine("Processing input " + i++);
            try {
            	logger.info("Deleting " + keyPrefix + input.getObjectId());
            	String objectId = input.getObjectId();
            	if("*".equals(objectId)) {
            		logger.fine("Found wildcard objectId - executing batch delete");
            		delete(redisConnection, keyPrefix);
            	} else {
            		delete(redisConnection, keyPrefix, objectId);
            	}
                response.addEmptyResult(input, OperationStatus.SUCCESS, "200", "OK");
            }
            catch (Exception e) {
                // make best effort to process every input
            	logger.log(Level.SEVERE, "Details of Exception:", e);
                ResponseUtil.addExceptionFailure(response, input, e);
            }
        }
		logger.fine("End of processing");
	}

	// Convenience methods for the connector operations
	public void delete(RedisConnection redisConnection, String keyPrefix) {
		redisConnection.delAll(keyPrefix + "*");
	}

	public void delete(RedisConnection redisConnection, String keyPrefix, String key) {
		String combinedKey = keyPrefix + ":" + key;
		redisConnection.del(combinedKey);
	}
}