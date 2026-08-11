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
example, `localhost:6379`). For **Single Endpoint**, provide the single endpoint.
For **OSS Cluster**, you may provide one or more comma-separated seed nodes
(`host1:port1,host2:port2,...`) — any reachable seed is enough, as the client
discovers the rest of the topology. Default is `localhost:6379`.
IPv6 literal addresses (e.g. `::1`) are not supported — use a hostname or an
IPv4 address.

**Clustering Policy** — Tells the connector how the target Redis presents its
topology, which determines the Redis client it uses. One of `Single Endpoint` or
`OSS Cluster`. Default is `Single Endpoint`. See
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
> OSS Cluster connections manage their own internal pool (see the note below the
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

> **Note:** The pooling fields above also apply to **OSS Cluster** connections, not
> just **Single Endpoint**. `JedisCluster` always maintains its own internal
> per-node connection pool regardless of **Enable Connection Pooling** — that
> field can't disable cluster-internal pooling, only change what it means:
>
> - **Enabled** — the cluster client (topology discovery plus its per-node pools)
>   is shared and reused across operation executions, using **Maximum
>   Connections** as the per-node limit, exactly like the standalone pooled path.
> - **Disabled** — a private, single-use cluster client is built fresh for each
>   execution and torn down immediately after, so its per-node pool only ever
>   needs one connection.
> - **Idle cleanup** — a shared pool or cluster client that no execution has used for
>   30 minutes is closed automatically and rebuilt on next use. Changing any connection
>   value (including credentials) starts a fresh pool immediately; the superseded pool
>   is cleaned up the same way.

## Clustering policy

Redis exposes its topology to clients in one of two ways at the protocol level:
either the client is responsible for sharding (it receives `MOVED`/`ASK`
redirects and must route to the owning shard), or a proxy hides sharding behind a
single endpoint. The **Clustering Policy** field tells the connector which to
expect so it selects the matching client. Choosing the wrong policy is the usual
cause of a `redis.clients.jedis.exceptions.JedisMovedDataException: MOVED ...`
error — a standalone client pointed at a client-sharded cluster cannot follow the
redirect.

**Single Endpoint** — The client talks to one address and does no cluster
routing. Choose this for a single Redis node (Azure Cache Basic/Standard, a
self-hosted single instance) or a proxy-fronted cluster that presents one
endpoint — including Redis Enterprise / Redis Cloud databases using the
"Enterprise" clustering policy and Azure Managed Redis configured with the
Enterprise clustering policy. The proxy hides sharding behind the single
endpoint, so the client treats it exactly like a single server; internally the
connector builds a standalone (optionally pooled) Jedis client. Default.

**OSS Cluster** — A client-sharded Redis Cluster that returns MOVED/ASK
redirects and expects the client to discover the topology and route keys
itself. Choose this for Redis OSS cluster mode, AWS ElastiCache with cluster
mode enabled, GCP Memorystore for Redis Cluster, Azure Managed Redis with the
OSS clustering policy, or a self-hosted cluster-enabled deployment. The
connector builds a cluster-aware JedisCluster client that follows redirects and
enumerates nodes (e.g. for `*` wildcard SCAN). Provide one or more
comma-separated seed nodes; any reachable seed is enough for topology
discovery.

How the policies map to the major offerings:

| Offering | Policy to select |
|----------|------------------|
| Single-node Redis (Azure Cache Basic/Standard, a self-hosted instance) | `Single Endpoint` |
| Azure Managed Redis / Redis Enterprise — **OSS** clustering policy | `OSS Cluster` |
| Azure Managed Redis / Redis Enterprise — **Enterprise** clustering policy | `Single Endpoint` |
| AWS ElastiCache with **cluster mode enabled** | `OSS Cluster` |
| AWS ElastiCache with **cluster mode disabled** | `Single Endpoint` |
| GCP Memorystore for Redis Cluster | `OSS Cluster` |
| GCP Memorystore for Redis (Basic/Standard) | `Single Endpoint` |
| Self-hosted Redis with `cluster-enabled yes` | `OSS Cluster` |

> **Note:** For an **OSS Cluster** connection over TLS to a managed cluster
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
| Clustering Policy | Single Endpoint |
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
| Clustering Policy | OSS Cluster |
| Use SSL | `true` |
| Authentication Type | Microsoft Entra Client Secret Credential |
| OAuth: Client ID | Application ID |
| OAuth: Client Secret | Application's Client Secret Value. Do not use Client Secret ID |
| OAuth: Scope | `https://redis.azure.com/.default` |
| OAuth: Access Token URL | `https://login.microsoftonline.com/{tenent-id}/oauth2/v2.0/token` for Azure Commercial |


