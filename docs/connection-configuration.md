# Redis connection

The Redis connection represents a single Redis endpoint, including the host(s),
transport security, authentication method, and connection-management settings.
If you use multiple Redis endpoints, or the same endpoint with different
credentials, you need a separate connection for each combination. You can pair one
connection with many Redis operations (GET, UPSERT, DELETE).

The connection supports standalone Redis, connection-pooled standalone Redis, and
clustered Redis — both client-sharded (OSS) and proxy-fronted (Enterprise) —
including Azure Cache for Redis and Azure Managed Redis with Microsoft Entra ID
authentication. The **Clustering Policy** field selects which topology the
connector expects; see [Clustering policy](#clustering-policy).

## Connection tab

**Hosts** — The Redis endpoint(s), each as `host:port` (note the colon; for
example, `localhost:6379`). For **Non-clustered** and **Enterprise Clustered**,
provide the single endpoint. For **OSS Clustered**, you may provide one or more
comma-separated seed nodes (`host1:port1,host2:port2,...`) — any reachable seed is
enough, as the client discovers the rest of the topology. Default is
`localhost:6379`.

**Clustering Policy** — Tells the connector how the target Redis presents its
topology, which determines the Redis client it uses. One of `Non-clustered`,
`OSS Clustered`, or `Enterprise Clustered`. Default is `Non-clustered`. See
[Clustering policy](#clustering-policy) for what each value means and how it maps
to the major managed-Redis offerings.

**Use SSL** — Enables TLS for the connection. Required for Azure Cache for Redis and Azure Managed Redis. Default is cleared (`false`).

**Authentication Type** — Selects how the connector authenticates to Redis. One of
`None`, `Basic`, or `Microsoft Entra Client Secret Credential`. Default is `None`.
See [Authentication](#authentication) for the fields each type requires.

**User** *(Basic only)* — The Redis AUTH username.

**Password** *(Basic only)* — The Redis AUTH password, or the Azure Cache for Redis
access key.

**Microsoft Entra OAuth 2.0 Credentials** *(Microsoft Entra only)* — The Boomi
OAuth 2.0 Connection Component that supplies the client credentials and token
endpoint. See [Microsoft Entra authentication settings](#microsoft-entra-authentication-settings).

**Connection Timeout (seconds)** — Maximum time to wait when establishing a
connection to Redis. Default is `5`. For Azure Redis with SSL/Auth, increase this
(for example, 30 seconds).

**Socket Read Timeout (seconds)** — Maximum time to wait for a read response from
Redis. Default is `5`. For Azure Redis with SSL/Auth, increase this (for example,
30 seconds).

**Enable Connection Pooling** — Reuses physical connections across process
invocations instead of opening a new connection each time. Default is cleared
(`false`).

> **Attention:** Disable connection pooling when the connector runs on the Boomi
> Public Runtime Cloud (Atom Cloud). This applies to the standalone connection path;
> OSS Clustered connections manage their own internal pool (see the note below the
> pooling fields).

**Maximum Connections** — The maximum number of connections the pool may hold.
Applies only when pooling is enabled. Default is `4`.

**Minimum Connections** — The minimum number of idle connections the pool
maintains. Applies only when pooling is enabled. Default is `1`.

**Maximum Idle Time (seconds)** — Idle connections are evicted from the pool after
this duration. Applies only when pooling is enabled. Default is `60`.

**Maximum Wait Time (seconds)** — Maximum time to wait for an available connection
from the pool before failing. Applies only when pooling is enabled. Default is
`5`.

> **Note:** The pooling fields above govern the **standalone** connection path
> (Non-clustered and Enterprise Clustered). An **OSS Clustered** connection always
> maintains its own internal per-node connection pool regardless of **Enable
> Connection Pooling** — when pooling is enabled it uses **Maximum Connections** as
> the per-node limit, otherwise a built-in default. Leaving **Enable Connection
> Pooling** off does not disable cluster pooling.

## Clustering policy

Redis exposes its topology to clients in one of two ways at the protocol level:
either the client is responsible for sharding (it receives `MOVED`/`ASK`
redirects and must route to the owning shard), or a proxy hides sharding behind a
single endpoint. The **Clustering Policy** field tells the connector which to
expect so it selects the matching client. Choosing the wrong policy is the usual
cause of a `redis.clients.jedis.exceptions.JedisMovedDataException: MOVED ...`
error — a standalone client pointed at a client-sharded cluster cannot follow the
redirect.

**Non-clustered** — A single Redis node. The connector uses a standalone (or
connection-pooled standalone) client. Default.

**OSS Clustered** — A client-sharded Redis Cluster that returns `MOVED`/`ASK`
redirects. The connector uses a cluster-aware client that discovers the topology
and routes each command to the owning shard.

**Enterprise Clustered** — A sharded cluster reached through a single proxy
endpoint that performs routing server-side and never returns `MOVED`. To the
client this behaves like a single endpoint, so the connector uses the same
standalone/pooled client as **Non-clustered**.

> **Note:** **Non-clustered** and **Enterprise Clustered** use the same
> single-endpoint connection under the hood; only **OSS Clustered** engages
> cluster-aware handling. All three are offered so the field matches the wording
> in the Azure portal and other managed-Redis consoles.

How the policies map to the major offerings:

| Offering | Policy to select |
|----------|------------------|
| Single-node Redis (Azure Cache Basic/Standard, a self-hosted instance) | `Non-clustered` |
| Azure Managed Redis / Redis Enterprise — **OSS** clustering policy | `OSS Clustered` |
| Azure Managed Redis / Redis Enterprise — **Enterprise** clustering policy | `Enterprise Clustered` |
| AWS ElastiCache with **cluster mode enabled** | `OSS Clustered` |
| AWS ElastiCache with **cluster mode disabled** | `Non-clustered` |
| GCP Memorystore for Redis Cluster | `OSS Clustered` |
| GCP Memorystore for Redis (Basic/Standard) | `Non-clustered` |
| Self-hosted Redis with `cluster-enabled yes` | `OSS Clustered` |

> **Note:** For an **OSS Clustered** connection over TLS to a managed cluster
> (e.g. Azure Managed Redis), the cluster-aware client discovers individual shard
> node addresses and connects to them directly. Confirm those node addresses are
> reachable from the runtime (Atom) and that TLS/SNI succeeds against them.

## Authentication

### None

Select **None** when the Redis server requires no authentication. No additional
fields are needed.

### Basic

Select **Basic** for username and password authentication. Populate the **User**
and **Password** fields on the Connection tab. For a Redis server that uses the
default user, set **User** to `default` and **Password** to the server password.

### Microsoft Entra authentication settings

Select **Microsoft Entra Client Secret Credential** to authenticate to Azure Cache
for Redis with Microsoft Entra ID.

This connector uses Boomi's native OAuth 2.0 credential management. The Boomi
platform handles token acquisition and refresh automatically — the connector does
not call the Azure AD token endpoint directly. For pooled connections, a fresh
token is obtained per new physical connection through a Jedis credentials
provider, so token rotation is handled automatically without rebuilding the pool.

> **Note:** The connector extracts the `oid` (Object ID) claim from the JWT access
> token and uses it as the Redis AUTH username, as Azure Cache for Redis requires.

To configure Microsoft Entra authentication:

1. Create an **Azure AD App Registration** with a client secret.
2. Grant the app the **Redis Cache Contributor** role on your Azure Cache for Redis
   instance.
3. Create a **Boomi OAuth 2.0 Connection Component** with:
   - **Grant Type** — `Client Credentials`
   - **Access Token URL** — pre-filled with the Azure **Commercial** default
     `https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token`. For Azure
     **Government**, change it to
     `https://login.microsoftonline.us/{tenantId}/oauth2/v2.0/token`. Replace
     `{tenantId}` with your Azure tenant ID.
   - **Client ID** and **Client Secret** — from your App Registration.
   - **Scope** — pre-filled with `https://redis.azure.com/.default`.
4. On the Redis connection, set **Authentication Type** to `Microsoft Entra Client
   Secret Credential` and select the OAuth 2.0 Connection Component in **Microsoft
   Entra OAuth 2.0 Credentials**.


#### Recommended connection settings for Azure Cache for Redis

| Field | Recommended value |
|-------|-------------------|
| Hosts | `<namespace>.redis.cache.windows.net:6380` |
| Clustering Policy | Non-clustered |
| Use SSL | `true` |
| Authentication Type | Microsoft Entra Client Secret Credential |
| OAuth: Client ID | Application ID |
| OAuth: Client Secret | Application's Client Secret Value. Do not use Client Secret ID |
| OAuth: Scope | `https://redis.azure.com/.default` |
| OAuth: Access Token URL | `https://login.microsoftonline.com/{tenent-id}/oauth2/v2.0/token` for Azure Commercial |


#### Recommended connection settings for Azure Managed Redis

| Field | Recommended value |
|-------|-------------------|
| Hosts | `<namespace>.<region>.redis.azure.net:10000` |
| Clustering Policy | OSS Clustered |
| Use SSL | `true` |
| Authentication Type | Microsoft Entra Client Secret Credential |
| OAuth: Client ID | Application ID |
| OAuth: Client Secret | Application's Client Secret Value. Do not use Client Secret ID |
| OAuth: Scope | `https://redis.azure.com/.default` |
| OAuth: Access Token URL | `https://login.microsoftonline.com/{tenent-id}/oauth2/v2.0/token` for Azure Commercial |


