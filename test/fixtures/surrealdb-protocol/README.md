# surrealdb-protocol conformance vectors

`test/surrealdb/codec/vectors_test.exs` runs the CBOR conformance vectors from
[surrealdb/surrealdb-protocol](https://github.com/surrealdb/surrealdb-protocol) at tag
`v0.14.0`: `cbor.yaml` as plain `cbor` and as `cbor.2`, and `cbor2.yaml` as `cbor.2`.

The vector files are not committed, because that tag is published under the Business Source
License 1.1. Download them into this directory before running the tests:

```sh
scripts/fetch-protocol-vectors.sh
```

The script pins each file's SHA-256. CI runs it before `mix test`. Without the files, the
conformance suite is skipped.

To move to a newer tag, change the tag and checksums in the script and the version in the
test.
