# Redis connection

The Redis connection represents a single Redis endpoint, including the host(s),
transport security, authentication method, and connection-management settings.
If you use multiple Redis endpoints, or the same endpoint with different
credentials, you need a separate connection for each combination. You can pair one
connection with many Redis operations (GET, UPSERT, DELETE).

The connection supports standalone Redis, connection-pooled standalone Redis, and
Redis Cluster — including Azure Cache for Redis with Microsoft Entra ID
authentication.

## Connection tab

**Hosts** — The Redis endpoint(s). Use `host:port` for a standalone server (for
example, `localhost:6379`), or a comma-separated list `host1:port1,host2:port2,...`
to connect to a Redis Cluster. Default is `localhost:6379`.

**Use SSL** — Enables TLS for the connection. Required for Azure Cache for Redis,
which listens on port `6380`. Default is cleared (`false`).

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
connection to Redis. Default is `30`.

**Socket Read Timeout (seconds)** — Maximum time to wait for a read response from
Redis. Default is `30`.

**Enable Connection Pooling** — Reuses physical connections across process
invocations instead of opening a new connection each time. Default is cleared
(`false`).

> **Attention:** Disable connection pooling when the connector runs on the Boomi
> Public Runtime Cloud (Atom Cloud).

**Maximum Connections** — The maximum number of connections the pool may hold.
Applies only when pooling is enabled. Default is `4`.

**Minimum Connections** — The minimum number of idle connections the pool
maintains. Applies only when pooling is enabled. Default is `1`.

**Maximum Idle Time (seconds)** — Idle connections are evicted from the pool after
this duration. Applies only when pooling is enabled. Default is `60`.

**Maximum Wait Time (seconds)** — Maximum time to wait for an available connection
from the pool before failing. Applies only when pooling is enabled. Default is
`60`.

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

> **Note:** The connector never hardcodes the token endpoint — it uses whatever
> Access Token URL the OAuth 2.0 component supplies. The connector descriptor
> pre-fills the Commercial URL and scope as editable defaults; sovereign clouds
> (Government, China, Germany) work by editing the Access Token URL.

#### Recommended connection settings for Azure Cache for Redis

| Field | Recommended value |
|-------|-------------------|
| Use SSL | `true` |
| Hosts | `<your-cache>.redis.cache.windows.net:6380` |
| Connection Timeout | `30` seconds |
| Socket Read Timeout | `30` seconds |
