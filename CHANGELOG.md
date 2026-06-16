# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Unreleased

### Added

- SurrealDB database client with supervised connection processes.
- WebSocket and HTTP transport engines, selected from the connection URL scheme.
- CBOR and JSON protocol codecs, with SurrealDB custom CBOR tags for native value types.
- Full RPC method coverage: `use`, `signin`, `signup`, `authenticate`, `invalidate`,
  `let`, `unset`, `query`, `select`, `create`, `insert`, `insert_relation`, `update`,
  `upsert`, `merge`, `patch`, `delete`, `relate`, `run`, `info`, `version`, `live`,
  `kill`, plus `export` and `import` over HTTP.
- Live queries delivered as process messages.
- Value types: `RecordId`, `Table`, `Uuid`, `Duration`, `Datetime`, `Decimal`,
  `Geometry`, `Range`, `Future`, `Bytes`, and `None`.
- Spectron memory client with the full tool surface: `remember`, `remember_many`,
  `recall`, `context`, `reflect`, `forget`, `chat` (with streaming), `state`, `profile`,
  `whoami`, `consolidate`, `elaborate`, `fsck`, `inspect`, `audit`, and `on_behalf_of`,
  plus the `documents`, `entities`, `sessions`, `lifecycle`, `traces`, `principals`,
  `scopes`, and `keys` namespaces.
