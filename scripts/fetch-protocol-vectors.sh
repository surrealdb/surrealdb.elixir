#!/bin/sh
# Downloads the SurrealDB CBOR conformance vectors that test/surrealdb/codec/vectors_test.exs
# runs. They come from the surrealdb-protocol repository at a fixed tag and are not committed
# here: that tag is published under the Business Source License 1.1. Each file is checked
# against its SHA-256 so a moved tag cannot change the tests.
set -eu

TAG=v0.14.0
BASE="https://raw.githubusercontent.com/surrealdb/surrealdb-protocol/$TAG/spec/vectors"
DEST="$(cd "$(dirname "$0")/.." && pwd)/test/fixtures/surrealdb-protocol"

mkdir -p "$DEST"

fetch() {
  file=$1
  sum=$2
  curl --silent --show-error --fail --location "$BASE/$file" --output "$DEST/$file.tmp"
  actual=$(shasum -a 256 "$DEST/$file.tmp" | cut -d ' ' -f 1)

  if [ "$actual" != "$sum" ]; then
    rm -f "$DEST/$file.tmp"
    echo "$file from surrealdb-protocol $TAG has SHA-256 $actual, expected $sum" >&2
    exit 1
  fi

  mv "$DEST/$file.tmp" "$DEST/$file"
  echo "fetched $file ($TAG)"
}

fetch cbor.yaml bffae26056974b10e2fddeb57a0fa2b8575cfd538c5208edaf6be6c94e5a2a83
fetch cbor2.yaml 02e45554bd54e9c7ae0ab254c0ce3b7854ae4478d6e1dcd4356c9bc30522abd4
