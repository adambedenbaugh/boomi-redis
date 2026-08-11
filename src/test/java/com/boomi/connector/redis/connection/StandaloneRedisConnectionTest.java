package com.boomi.connector.redis.connection;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import redis.clients.jedis.*;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class StandaloneRedisConnectionTest {

    private RedisConnectionConfig standaloneConfig() {
        PropertyMap props = mock(PropertyMap.class);
        when(props.getProperty("hosts")).thenReturn("localhost:6379");
        when(props.getProperty("authenticationType")).thenReturn("None");
        when(props.getProperty("clusteringPolicy")).thenReturn("NonClustered");
        BrowseContext ctx = mock(BrowseContext.class);
        when(ctx.getConnectionProperties()).thenReturn(props);
        return new RedisConnectionConfig(ctx);
    }

    @Test
    public void delAllUsesPipelinedSingleKeyDeletes() {
        Jedis jedis = mock(Jedis.class);
        Pipeline pipeline = mock(Pipeline.class);
        when(jedis.pipelined()).thenReturn(pipeline);
        when(jedis.scan(eq(ScanParams.SCAN_POINTER_START), any(ScanParams.class)))
                .thenReturn(new ScanResult<>(ScanParams.SCAN_POINTER_START, Arrays.asList("a", "b")));
        JedisClientFactory factory = mock(JedisClientFactory.class);
        when(factory.createClient(any(HostAndPort.class), any(JedisClientConfig.class))).thenReturn(jedis);

        StandaloneRedisConnection conn = new StandaloneRedisConnection(standaloneConfig(), factory);
        conn.delAll("prefix:");

        verify(pipeline).del("a");
        verify(pipeline).del("b");
        verify(pipeline).sync();
        verify(jedis, never()).del(any(String[].class));
        conn.close();
    }

    @Test
    public void getAllEscapesGlobMetacharactersInPrefixBeforeScan() {
        Jedis jedis = mock(Jedis.class);
        ArgumentCaptor<ScanParams> captor = ArgumentCaptor.forClass(ScanParams.class);
        when(jedis.scan(eq(ScanParams.SCAN_POINTER_START), captor.capture()))
                .thenReturn(new ScanResult<>(ScanParams.SCAN_POINTER_START, Collections.emptyList()));
        JedisClientFactory factory = mock(JedisClientFactory.class);
        when(factory.createClient(any(HostAndPort.class), any(JedisClientConfig.class))).thenReturn(jedis);

        StandaloneRedisConnection conn = new StandaloneRedisConnection(standaloneConfig(), factory);
        conn.getAll("cache[1]:");

        assertEquals("cache\\[1\\]:*", captor.getValue().match());
        conn.close();
    }
}
