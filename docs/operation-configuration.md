# Redis operation

The Redis operation defines how the connector interacts with your Redis cache. Each
operation is a distinct action, and you create a separate operation component for
each action you want to perform. After you define an operation, you use it in a
Connector step in a process.

The connector supports the following actions:

- **GET** — Retrieves one cache entry by key, or all entries matching a prefix.
- **UPSERT** — Creates or updates a cache entry.
- **DELETE** — Deletes one cache entry by key, or all entries matching a prefix.

Each operation resolves a Redis key from the object ID supplied at runtime. Setting
the object ID to `*` applies the operation to all keys matching the configured key
prefix (GET all / DELETE all).

## GET

The GET operation retrieves cache entries. For a single get, set the object ID to
the key (without the prefix). For a get-all, set the object ID to `*` to return
every entry whose key matches the configured prefix.

### Options tab

**Key Prefix** — Prepended to the object ID before the lookup. Default is empty.

**Remove Key Prefix from Response** — When enabled, the configured prefix is
stripped from the key names in the response. Default is selected (`true`).

**Throw Exception when not found** — When enabled, a missing key returns a failure
instead of an empty result. Default is cleared (`false`). This also applies to a
get-all (`*`): when no keys match the prefix, the operation returns a failure if
this option is enabled, and a success with **no output document** if it is
disabled. (Versions before 1.0.1 ignored this option for get-all and returned an
empty `[]` document on no matches.)

### Response profile

GET returns a JSON array of `{key, value}` entries. A single get returns an array
containing one entry; a get-all returns one entry per matching key.

```json
[
  { "key": "mykey", "value": "myvalue" }
]
```

## UPSERT

The UPSERT operation creates a cache entry, or updates it if the key already
exists.

### Options tab

**Key Prefix** — Prepended to the key before writing. Default is empty.

**Set Time To Live (TTL)** — Expiry for the entry, in milliseconds. Set to `-1` to
disable expiry. This value can be overridden at runtime. Default is `-1`.

### Request profile

UPSERT accepts a JSON object with the key and value to write.

```json
{ "key": "mykey", "value": "myvalue" }
```

## DELETE

The DELETE operation removes cache entries. For a single delete, set the object ID
to the key. To delete every entry matching the configured prefix, set the object ID
to `*`.

### Options tab

**Key Prefix** — Prepended to the key before deletion. When the object ID is `*`,
the prefix defines the pattern of keys to delete. Default is empty.

> **Attention:** Setting the object ID to `*` with an empty key prefix deletes every
> key matching the prefix pattern. Confirm the prefix before running a delete-all
> against a shared cache.
