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

## LISTEN

The LISTEN operation consumes messages from a Redis List or Stream. It works as a
listener shape in Boomi: each message is delivered to the process once it arrives
(or is redelivered on process failure, for Stream mode).

### Options tab

**Queue Type** — The Redis data structure used for the queue. Default is `List`.
- `List` — Simple, at-most-once delivery via `BLPOP`. A popped message is gone
  from the queue even if the receiving process run crashes.
- `Stream` — Reliable, at-least-once delivery via a Redis Stream consumer group.
  An unacknowledged message is automatically redelivered, and a message that
  keeps failing is moved to a dead-letter stream instead of retried forever.
  Requires "Consumer Group Name" set on the connection.

**Queue Key** — The Redis key to consume (the List or Stream name). Default is
empty.

**Singleton Mode** — When enabled, only one node in a Molecule/clustered Atom
runs this listener at a time. When disabled (default), every node listens
independently — Redis's own atomicity ensures a given message is still delivered
to only one node. Default is cleared (`false`).

> **Note:** Enforcing single-node execution also requires the container property
> `com.boomi.container.bounded.listen.enabled=true`. Without it, enabling this
> checkbox falls back to non-singleton behavior — every node still listens. This
> remains safe (Redis's own atomicity still prevents double-delivery), but is not
> what enabling the checkbox implies.

**Ack Mode** *(Stream only)* — How a message is acknowledged after process
completion. Default is `On process completion`.
- `On process completion` — Acknowledges a message only after its process run
  finishes successfully. True at-least-once delivery, but the listener is blocked
  waiting for the process to complete.
- `On submission` — Acknowledges as soon as Boomi accepts the message. Faster,
  but a process failure after acceptance is not redelivered.

**Idle Reclaim Timeout (ms)** *(Stream only)* — A pending message idle longer
than this duration (in milliseconds) is reclaimed for redelivery (or dead-lettered
if Max Delivery Attempts is exceeded). In "On Process Completion" Ack Mode, this
value is also the maximum time the listener waits for a process execution to
finish; if the process takes longer, it is abandoned and the message is left
unacknowledged for reclaim. Default is `60000` (60 seconds).

**Max Delivery Attempts** *(Stream only)* — Beyond this many delivery attempts, a
message is moved to the dead-letter stream instead of redelivered again. Default
is `5`.

**Dead Letter Key** *(Stream only)* — Redis Stream key for messages that exceeded
Max Delivery Attempts. Defaults to `<Queue Key>:dead-letter` when left blank.
Default is empty.

**Poll Block (ms)** — Blocking timeout per poll iteration (BLPOP timeout / XREADGROUP BLOCK), in milliseconds. Default is `5000` (5 seconds).

> **Attention:** On a pooled connection, a Listen operation borrows one connection
> from the shared pool and holds it for essentially the entire time it is running
> — a blocking poll borrows-and-holds it for the block duration, iteration after
> iteration. Size the connection's **Maximum Connections** to accommodate the
> number of concurrent Listen operations plus normal GET/UPSERT/DELETE/Send
> traffic sharing that same connection.

## Send

The Send operation produces messages to a Redis List or Stream, paired with a
Listen operation (consumer) on the same queue key. It is built on the connector
SDK's `EXECUTE` operation type, displayed as "Send" in the operation picker.

### Options tab

**Queue Type** — The Redis data structure used for the queue. Default is `List`.
- `List` — Pushes with `RPUSH`. Pair with a List-mode Listen for FIFO delivery
  order.
- `Stream` — Pushes with `XADD`. Pair with a Stream-mode Listen.

**Key Prefix** — Prepended to the key before pushing/adding. Default is empty.
