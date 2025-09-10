package com.boomi.connector.redis;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.redis.operation.RedisDeleteOperation;
import com.boomi.connector.redis.operation.RedisGetOperation;
import com.boomi.connector.redis.operation.RedisUpsertOperation;
import com.boomi.connector.util.BaseConnector;

/**
 * 
 * @author anthony.rabiaza@gmail.com
 *
 */
public class RedisConnector extends BaseConnector {

    @Override
    public Browser createBrowser(BrowseContext context) {
        return new RedisBrowser(createConnection(context));
    }    

    @Override
    protected Operation createGetOperation(OperationContext context) {
        return new RedisGetOperation(context);
    }

    @Override
    protected Operation createUpsertOperation(OperationContext context) {
        return new RedisUpsertOperation(context);
    }

    @Override
    protected Operation createDeleteOperation(OperationContext context) {
        return new RedisDeleteOperation(context);
    }
   
    private RedisConnection createConnection(BrowseContext context) {
        return new RedisConnection(context);
    }
}