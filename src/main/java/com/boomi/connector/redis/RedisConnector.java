package com.boomi.connector.redis;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.listen.ListenConnector;
import com.boomi.connector.api.listen.ListenOperation;
import com.boomi.connector.redis.operation.RedisDeleteOperation;
import com.boomi.connector.redis.operation.RedisGetOperation;
import com.boomi.connector.redis.operation.RedisListenOperation;
import com.boomi.connector.redis.operation.RedisSendOperation;
import com.boomi.connector.redis.operation.RedisUpsertOperation;
import com.boomi.connector.redis.pool.RedisListenManager;
import com.boomi.connector.util.BaseConnector;

/**
 *
 * @author anthony.rabiaza@gmail.com
 *
 */
public class RedisConnector extends BaseConnector implements ListenConnector<RedisListenManager> {

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

    @Override
    protected Operation createExecuteOperation(OperationContext context) {
        return new RedisSendOperation(context);
    }

    @Override
    public ListenOperation<RedisListenManager> createListenOperation(OperationContext context) {
        return new RedisListenOperation(context);
    }

    @Override
    public RedisListenManager createListenManager(ConnectorContext context) {
        return new RedisListenManager();
    }

    private RedisConnection createConnection(BrowseContext context) {
        return new RedisConnection(context);
    }
}