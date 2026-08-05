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

## Connection Configuration

| Field | Description | Default |
|-------|-------------|---------|
| **Hosts** | `host:port` for standalone; `host1:port1,host2:port2,...` for cluster | `localhost:6379` |
| **Use SSL** | Enable TLS. Required for Azure Cache for Redis (port 6380). | `false` |
| **Authentication Type** | `None`, `Basic`, or `Microsoft Entra Client Secret Credential` | `None` |
| **User** *(Basic only)* | Redis AUTH username | |
| **Password** *(Basic only)* | Redis AUTH password / Azure access key | |
| **Microsoft Entra OAuth 2.0 Credentials** *(Entra only)* | Boomi OAuth 2.0 Connection Component (see below) | |
| **Connection Timeout (seconds)** | Max time to establish connection | `30` |
| **Socket Read Timeout (seconds)** | Max time to wait for a read | `30` |
| **Enable Connection Pooling** | Reuse connections across invocations | `false` |
| **Maximum Connections** | Pool max size | `4` |
| **Minimum Connections** | Pool min idle | `1` |
| **Maximum Idle Time (seconds)** | Evict idle connections after this duration | `60` |
| **Maximum Wait Time (seconds)** | Max time to wait for a pool connection | `60` |

> **Atom Cloud note:** Disable connection pooling when running on the Boomi Public Runtime Cloud.

## Operations

### GET

Retrieves one or all cache entries.

| Property | Description | Default |
|----------|-------------|---------|
| **Key Prefix** | Prepended to the object ID before lookup | *(empty)* |
| **Remove Key Prefix from Response** | Strip prefix from returned key names | `true` |
| **Throw Exception when not found** | Return a failure when the key is missing | `false` |

- **Single get**: Set the object ID to the key (without prefix). Returns a JSON array with one `{key, value}` entry.
- **Get all**: Set the object ID to `*`. Returns a JSON array of all `{key, value}` pairs matching the prefix.

**Response profile** (JSON array):
```json
[
  { "key": "mykey", "value": "myvalue" }
]
```

### UPSERT

Creates or updates a cache entry.

| Property | Description | Default |
|----------|-------------|---------|
| **Key Prefix** | Prepended to the key | *(empty)* |
| **Set Time To Live (TTL)** | Expiry in milliseconds; `-1` to disable. Overridable. | `-1` |

**Request profile** (JSON object):
```json
{ "key": "mykey", "value": "myvalue" }
```

### DELETE

Deletes one or all cache entries.

| Property | Description | Default |
|----------|-------------|---------|
| **Key Prefix** | Prepended to the key | *(empty)* |

- **Single delete**: Set the object ID to the key.
- **Delete all**: Set the object ID to `*`. Deletes all keys matching the prefix pattern.

## Microsoft Entra Authentication (Azure Cache for Redis)

This connector uses **Boomi's native OAuth 2.0 credential management**. The Boomi platform handles token acquisition and refresh automatically — the connector does not call the Azure AD token endpoint directly. For pooled connections, a fresh token is obtained per new physical connection via a Jedis credentials provider, so token rotation is handled automatically without pool rebuilds.

### Setup

1. **Create an Azure AD App Registration** with a client secret.
2. **Grant it the "Redis Cache Contributor" role** on your Azure Cache for Redis instance.
3. **Create a Boomi OAuth 2.0 Connection Component** with:
   - Grant type: `Client Credentials`
   - Access Token URL — pre-filled with the Azure **Commercial** default `https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token`. For Azure **Government**, change it to `https://login.microsoftonline.us/{tenantId}/oauth2/v2.0/token`. Replace `{tenantId}` with your Azure tenant ID.
   - Client ID and Client Secret from your App Registration
   - Scope — pre-filled with `https://redis.azure.com/.default`
4. **In the Redis Connector connection**, set Authentication Type to `Microsoft Entra Client Secret Credential` and select the OAuth 2.0 credential component.

> The connector never hardcodes the token endpoint — it uses whatever Access Token URL the OAuth 2.0 component supplies. The connector descriptor pre-fills the Commercial URL and scope as editable defaults; sovereign clouds (Government, China, Germany) work by editing the Access Token URL.

The connector extracts the `oid` (Object ID) claim from the JWT access token to use as the Redis AUTH username, as required by Azure Cache for Redis.

### Recommended connection settings for Azure

| Field | Recommended value |
|-------|------------------|
| Use SSL | `true` |
| Hosts | `<your-cache>.redis.cache.windows.net:6380` |
| Connection Timeout | `30` seconds |
| Socket Read Timeout | `30` seconds |

## Updating the Connector Icon

The connector icon can be updated using the Boomi Connector Icon API.

1. Open `assets/postman/boomi_redis_connector.postman_collection.json` in Postman.
2. Set Basic auth credentials in the **Authorization** tab (username: `BOOMI_TOKEN.<username>`).
3. Set the `classificationType` and `baseUrlConnector` variables in the **Variables** tab.
4. Attach `assets/postman/redis.svg` to the `connectorIcon` field in the request body.
5. Send the request and clear the browser cache to see the updated icon.

![Postman auth setup](assets/postman-auth.png)
![Postman variables](assets/postman-variables.png)
![Postman body](assets/postman-body.png)

---

*Based on [BoomiCacheConnector](https://bitbucket.org/officialboomi/boomicacheconnector/src/master/) by anthony.rabiaza@gmail.com.*
