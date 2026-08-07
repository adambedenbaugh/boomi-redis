# Redis Custom Connector for Boomi

Technical documentation for the Boomi Redis Connector — a Boomi custom connector
that performs cache operations against Redis from integration processes and APIs.
It supports standalone, connection-pooled standalone, and clustered Redis
(OSS and Enterprise clustering policies) deployments, including Azure Cache for
Redis and Azure Managed Redis with Microsoft Entra ID authentication.

For the top-level project overview, see the [project README](../README.md).

## Documentation

| Page | What it covers |
|------|----------------|
| [Getting Started](getting-started.md) | Develop, build, and test the Redis custom connector — from a fresh clone to uploadable artifacts. |
| [Connection Configuration](connection-configuration.md) | Every connection field, the clustering policies (Single Endpoint, OSS Cluster), and the three authentication modes (None, Basic, Microsoft Entra). |
| [Operation Configuration](operation-configuration.md) | The GET / UPSERT / DELETE operations, their properties, and JSON request/response profiles. |
| [Deploying connector to the Boomi Platform](https://developer.boomi.com/docs/Connectors/DeployConnectors/Versioning_and_releasing_connector)|Boomi's official documentation on deploying a connector to Boomi Integration. |
| [Updating the Connector Icon](updating-connector-icon.md) | How to update the connector's icon. |

## At a glance

| | |
|---|---|
| **Operations** | GET, UPSERT, DELETE — single key and `*` wildcard |
| **Authentication** | None · Basic (user/password) · Microsoft Entra ID (OAuth 2.0 client credentials) |
| **Deployment modes** | Standalone · connection-pooled standalone · clustered |
| **Clustering Policy** | Single Endpoint · OSS Cluster |
| **SSL/TLS** | Configurable; required for Azure Cache for Redis (port 6380) |
| **Connector SDK** | Boomi Connector SDK 2.22.1 |
| **Redis client** | Jedis 5.2.0 (Redis 6+ compatible) |
| **Language / build** | Java 8 · Gradle (`com.boomi.connector` plugin) |

## Building

```bash
./gradlew build
```

This compiles the connector, runs the unit tests, and copies two uploadable
artifacts to `build/connector-upload/`:

- `BoomiRedisConnector-<version>.car` — the connector archive
- `connector-descriptor.xml` — the field/operation definitions

### Run unit tests (no Redis required)

```bash
./gradlew test
```

### Run integration tests (requires live Redis or Docker)

Copy the relevant properties template from `src/test/resources/` and populate it
with your connection details, then:

```bash
./gradlew integrationTest
```

See [Getting Started](getting-started.md) for the full build, test, and setup
walkthrough.

## Installing in Boomi

1. Run `./gradlew build` to produce the artifacts in `build/connector-upload/`.
2. In the Boomi Enterprise Platform, navigate to **Settings → Developer**.
3. Create a new connector entry (or update an existing one).
4. Upload `BoomiRedisConnector-<version>.car` and `connector-descriptor.xml`.

Optionally update the connector icon — see
[Updating the Connector Icon](updating-connector-icon.md).

## External references

- Boomi Connector SDK 2.22.1 Javadoc —
  https://boomisdkjavadoc.s3.amazonaws.com/javadoc/2.22.1/index.html
- Jedis (Redis Java client) — https://github.com/redis/jedis
- Azure Cache for Redis with Microsoft Entra ID —
  https://learn.microsoft.com/azure/azure-cache-for-redis/cache-azure-active-directory-for-authentication
