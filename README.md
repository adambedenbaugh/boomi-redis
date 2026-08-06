# Boomi Redis Connector

A Boomi custom connector for Redis that enables high-throughput caching operations from Boomi integration processes and APIs. Supports standalone Redis, pooled standalone Redis, and Redis Cluster deployments — including Azure Cache for Redis with Microsoft Entra authentication.

## Features

- **Operations**: GET, UPSERT, DELETE (single key and wildcard)
- **Authentication**: None, Basic (username/password), Microsoft Entra ID (OAuth 2.0 client credentials)
- **Deployment modes**: Standalone, connection-pooled standalone, Cluster
- **SSL/TLS**: Configurable; required for Azure Cache for Redis (port 6380)
- **Key prefixes**: Configurable per operation; prefix can be stripped from GET responses
- **TTL**: Configurable per UPSERT operation (in milliseconds); overridable at runtime
- **JSON profiles**: Request/response schemas auto-generated via Browse

## Connector Architecture

- **SDK**: Boomi Connector SDK 2.22.1
- **Redis client**: Jedis 5.2.0 (Redis 6+ compatible)
- **Build**: Gradle with `com.boomi.connector` plugin

## Building

```bash
./gradlew build
```

This produces the connector archive and copies the descriptor to `build/connector-upload/`.

### Run unit tests (no Redis required)

```bash
./gradlew test
```

### Run integration tests (requires live Redis)

Copy the relevant properties file template from `src/test/resources/` and populate it with your connection details, then:

```bash
./gradlew integrationTest
```

## Installing in Boomi

1. Run `./gradlew build` to produce the artifacts in `build/connector-upload/`.
2. In the Boomi Enterprise Platform, navigate to **Settings → Developer**.
3. Create a new connector entry (or update an existing one).
4. Upload `BoomiRedisConnector-<version>.car` and `connector-descriptor.xml`.

Optionally update the connector icon using the Postman collection in `assets/postman/`.








---

*Fork of [BoomiCacheConnector](https://bitbucket.org/officialboomi/boomicacheconnector/src/master/) by anthony.rabiaza@gmail.com.*
