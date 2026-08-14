# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and this project adheres to Semantic Versioning.


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