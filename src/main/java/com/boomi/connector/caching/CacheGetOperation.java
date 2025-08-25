package com.boomi.connector.caching;

import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import com.boomi.connector.api.GetRequest;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.util.BaseGetOperation;


/**
 * 
 * @author anthony.rabiaza@gmail.com
 *
 */
public class CacheGetOperation extends BaseGetOperation {

	protected CacheGetOperation(CacheConnection conn) {
		super(conn);
	}

	@Override
	protected void executeGet(GetRequest request, OperationResponse response) {

		Logger logger = response.getLogger();
		logger.fine("executeGet");

		String cacheName = getContext().getOperationProperties().getProperty("cache_name");
		logger.fine("CacheName: " + cacheName);
		
		boolean autoKey = getContext().getOperationProperties().getBooleanProperty("auto_key");
		logger.fine("AutoKey: " + autoKey);

		boolean throwException = getContext().getOperationProperties().getBooleanProperty("throw_exception");
		logger.fine("ThrowException: " + throwException);

		boolean wrapInProfile = getContext().getOperationProperties().getBooleanProperty("wrap_inprofile");
		logger.fine("WrapInProfile: " + wrapInProfile);

		long ttl;
		try {
			ttl = getContext().getOperationProperties().getLongProperty("set_ttl");
		} catch (Exception e) {
			ttl = -1;
		}
		logger.fine("TTL: " + ttl);
		
		ObjectIdData input = request.getObjectId();

		try {
			String objectId = input.getObjectId();

			logger.fine("ID: " + objectId);

			if(!"*".equals(objectId)) {//Normal Get
				String cachedValue = getConnection().get(cacheName, objectId, ttl);

				if(throwException && cachedValue==null) {
					ResponseUtil.addExceptionFailure(response, input, new Exception("Value not found in the Cache"));
				}

				logger.fine("CacheValue received:" + cachedValue);

				if(cachedValue != null) {
					logger.fine("Cache Hit");
					if(wrapInProfile) {
						response.addResult(input, OperationStatus.SUCCESS, "200", "OK", ResponseUtil.toPayload("<Get><ID>"+objectId+"</ID><Value>"+cachedValue+"</Value></Get>"));
					} else {
						response.addResult(input, OperationStatus.SUCCESS, "200", "OK", ResponseUtil.toPayload(cachedValue));
					}
				} else {
					//Return null
					logger.fine("Cache Empty");
					response.addEmptyResult(input, OperationStatus.SUCCESS, "200", "OK");
				}

			} else {//Get All
				Map<String,String> cachedValue = getConnection().get(cacheName, ttl);
				System.out.println("cachedValue: " + cachedValue);
				if(throwException && cachedValue==null) {
					ResponseUtil.addExceptionFailure(response, input, new Exception("Value not found in the Cache"));
				}

				logger.fine("CacheValue received: " + cachedValue);

				if(cachedValue != null) {
					logger.fine("Cache Hit");
					for (Iterator<String> iterator = cachedValue.keySet().iterator(); iterator.hasNext();) {
						String key = iterator.next();
						String value = cachedValue.get(key);

						if(wrapInProfile) {
							response.addPartialResult(input, OperationStatus.SUCCESS, "200", "OK", ResponseUtil.toPayload("<Get><ID>"+key+"</ID><Value>"+value+"</Value></Get>"));
						} else {
							response.addPartialResult(input, OperationStatus.SUCCESS, "200", "OK", ResponseUtil.toPayload(value));
						}
					}
					
					response.finishPartialResult(input);
				} else {
					//Return null
					logger.fine("Cache Empty");
					response.addEmptyResult(input, OperationStatus.SUCCESS, "200", "OK");
				}
			}

		}catch (Exception e) {
			ResponseUtil.addExceptionFailure(response, input, e);
		}
	}

	@Override
	public CacheConnection getConnection() {
		return (CacheConnection) super.getConnection();
	}
}