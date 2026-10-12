# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and this project adheres to Semantic Versioning.


## [1.3.0]

### Added
- A `LISTEN` operation that consumes messages from a Redis List (simple, at-most-once via `BLPOP`)
  or a Redis Stream consumer group (reliable, at-least-once, with idle-reclaim and dead-lettering
  after a configurable number of delivery attempts), and a matching Send producer operation
  (`RPUSH`/`XADD`, built on the connector SDK's `EXECUTE` operation type and displayed as "Send").
  Send also supports a Key Prefix field, matching GET/UPSERT/DELETE. See
  `docs/operation-configuration.md`.

### Fixed
- Send's List mode (`RPUSH`) now raises a descriptive error when the target key already holds a
  non-List value in Redis, instead of a raw `WRONGTYPE` exception.
- Listen now self-heals from a broken Redis connection (broken pipe, connection reset, read
  timeout) by automatically closing and rebuilding it, instead of retrying the same dead socket
  forever. Repeated failures during a sustained outage are now throttled to at most one logged
  entry per 5 minutes (previously logged unconditionally every 5-second retry, which could fill
  the container log at scale — one production incident logged ~4800 identical stack traces over
  ~7 hours before a manual listener restart).
- Listen's Stream reclaim sweep now dead-letters a message on the correct delivery attempt.
  `Max Delivery Attempts = 5` previously allowed 6 deliveries before dead-lettering, because the
  guard compared against the delivery count read from `XPENDING` *before* the sweep's `XCLAIM`
  call — but `XCLAIM` increments Redis's delivery counter as a side effect of claiming, so that
  pre-claim count was always one lower than the count that actually governed the delivery about
  to happen.
- Listen: the poll loop now reports `OFFLINE` to the runtime for *any* failure, including `Error`s
  (previously an `Error` silently killed the poll thread while the listener still showed ONLINE).
  OFFLINE messages fall back to the exception class name when the exception has no message.

## [1.2.0] - 2026-08-14

### Changed
- Upgraded the Boomi Connector SDK from 2.22.1 to 2.30.1 (`ResponseUtil` moved to
  `com.boomi.connector.util`; no behavior change).

### Tests
- Integration tests are now fully self-contained: every test spins up its own throwaway Redis
  via Testcontainers, so Docker is the only prerequisite — no properties files, no live
  endpoints, no credentials on disk. The two legacy live-endpoint tests were replaced with
  Testcontainers equivalents that drive UPSERT/GET through the SDK's `ConnectorTester` harness
  for both Basic and Microsoft Entra authentication.
- Added a TLS integration test: a per-run self-signed certificate (generated with the JDK's
  keytool, trusted via runtime `SSLContext` injection) proves `useSSL` end-to-end against a
  TLS-only Redis, including a negative control showing plaintext connections are rejected.
- The browse test no longer needs Docker or config files and runs with the unit suite.

## [1.1.0] - 2026-08-14

### Added
- Shared connection pooling across executions: pooled standalone and OSS Cluster clients are
  managed by a singleton pool registry (modeled on the official JMS V2 connector), keyed by the
  connection's configuration values; idle clients are closed automatically after 30 minutes.
- Changing connection settings (including a rotated password or client secret) rebuilds the
  shared pool automatically — no Atom restart needed. Microsoft Entra token refresh reuses the
  existing pool.
- GET and DELETE reject a missing/empty key with a descriptive error instead of silently
  targeting the wrong key.

### Changed
- Clustering Policy options renamed to **Single Endpoint** and **OSS Cluster**; the legacy
  `EnterpriseClustered` value maps automatically to Single Endpoint.
- Wildcard GET with no matches now returns no output document (previously an empty `[]` JSON
  document); with Throw Exception enabled it fails the document.

### Fixed
- Microsoft Entra with connection pooling: executions after the original token expired failed
  with `WRONGPASS invalid username-password pair`; token fetches now always go through the
  newest execution's OAuth context.
- Wildcard GET/DELETE on OSS Cluster leaked one pooled connection per node per execution,
  eventually exhausting the shared cluster client.
- The pool-eviction thread now stops itself when no shared clients remain, so redeploying the
  connector no longer leaks a thread or pins the old deployment until Atom restart.
- Unpooled OSS Cluster connections open one connection per node instead of eight.
- Key prefixes containing glob metacharacters (`* ? [ ]`) now match literally in wildcard
  operations; UTF-8 is used explicitly for all string encoding.
- Connections are closed after every execution; Upsert validates its input document.
- Hardened error handling throughout: connection failures, malformed or missing Entra token
  claims, unrecognized authentication types, and port parsing all fail with descriptive,
  actionable errors instead of raw exceptions — and secret values are never written to logs.

## [1.0.0] - 2026-08-06

### Added
- Refactored connector.
- Added Microsoft Entra authentication support.
- Added support for Azure Cache for Redis and Azure Managed Redis.

### Changed
- BREAKING CHANGE: Supports JSON request and response profiles.
- BREAKING CHANGE: Removed support for setting Dynamic Process Properties.

## [0.90] - 2025-07-24

- Initial fork of [BoomiCacheConnector](https://bitbucket.org/officialboomi/boomicacheconnector/src/master/)