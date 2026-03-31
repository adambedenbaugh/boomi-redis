# Redis Connection Architecture

This directory contains the factory-based Redis connection implementation. Each variation of connection type (clustered vs standalone, and pooled vs non-pooled) uses different classes. This architecture was used to simplify the individual cconnection classes. 

## Architecture

```
RedisConnectionInterface (interface)
└── BaseRedisConnection (abstract base class)
    ├── StandaloneRedisConnection (direct connection, no pooling)
    ├── StandalonePooledRedisConnection (JedisPool-based)
    └── ClusteredRedisConnection (JedisCluster-based)
```