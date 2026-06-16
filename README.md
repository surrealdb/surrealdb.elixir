# SurrealDB for Elixir

An Elixir SDK for [SurrealDB](https://surrealdb.com) and
[Spectron](https://surrealdb.com/platform/spectron), built for idiomatic Elixir: supervised
connections you pass around as handles, native value types, live queries delivered as
process messages, and a typed Spectron memory client.

It aims for feature parity with the official JavaScript SDKs (`surrealdb` and
`@surrealdb/spectron`).

## Features

- **Two transport engines**, chosen automatically from the connection URL scheme:
  WebSocket (`ws`/`wss`) and HTTP (`http`/`https`).
- **Two protocols**: CBOR (default, full type fidelity) and JSON.
- **Native value types**: `RecordId`, `Table`, `Uuid`, `Duration`, `Datetime`, `Decimal`,
  `Geometry`, `Range`, `Bytes`, and `None`.
- **Full RPC surface**: `use`, `signin`, `signup`, `authenticate`, `invalidate`, `let`,
  `unset`, `query`, `select`, `create`, `insert`, `insert_relation`, `update`, `upsert`,
  `merge`, `patch`, `delete`, `relate`, `run`, `info`, `version`, plus `export`/`import`.
- **Live queries** delivered to any process as messages.
- **Spectron client** with the complete tool surface and namespaces.
- Works in any Elixir project: it is a plain library with a supervisable connection
  process and no framework assumptions.

## Installation

Add `:surrealdb` to the dependencies in your `mix.exs`:

```elixir
def deps do
  [
    {:surrealdb, "~> 0.1"}
  ]
end
```

Then fetch dependencies with your usual workflow.

## Connecting

Start a connection and keep the returned handle. It is the pid (or registered name) of a
supervised process, so you pass it as the first argument to every call.

```elixir
{:ok, db} =
  SurrealDB.start_link(
    url: "ws://localhost:8000/rpc",
    namespace: "test",
    database: "test",
    auth: %SurrealDB.Auth.Root{user: "root", pass: "root"}
  )
```

Options:

- `:url` (required) - for example `"ws://localhost:8000/rpc"` or `"https://db.example/rpc"`.
- `:namespace` / `:database` - selected on connect via `use`.
- `:auth` - a `SurrealDB.Auth` struct to sign in with on connect.
- `:codec` - `:cbor` (default) or `:json`.
- `:name` - register the process under a name.
- `:transport_opts` - extra transport options, for example TLS settings.

### In a supervision tree

```elixir
children = [
  {SurrealDB.Connection,
   url: "wss://db.example/rpc",
   namespace: "app",
   database: "prod",
   auth: %SurrealDB.Auth.Database{namespace: "app", database: "prod", user: "u", pass: "p"},
   name: MyApp.SurrealDB}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

Now any process can call `SurrealDB.query(MyApp.SurrealDB, "...")`.

## CRUD and queries

```elixir
# Create returns the created record(s)
{:ok, [tobie]} = SurrealDB.create(db, "person", %{name: "Tobie", age: 30})

# Select a whole table, a single record, or a range
{:ok, people} = SurrealDB.select(db, "person")
{:ok, one} = SurrealDB.select(db, "person:tobie")

# Update, merge, patch, delete
{:ok, _} = SurrealDB.merge(db, "person:tobie", %{active: true})
{:ok, _} = SurrealDB.patch(db, "person:tobie", [%{"op" => "replace", "path" => "/age", "value" => 31}])
{:ok, _} = SurrealDB.delete(db, "person:tobie")

# Parameterised queries. Each statement's result is returned in order.
{:ok, [rows]} = SurrealDB.query(db, "SELECT * FROM person WHERE age > $min", %{min: 18})

# Graph edges
{:ok, _} = SurrealDB.relate(db, "person:tobie", "wrote", "article:surreal", %{year: 2024})

# Functions
{:ok, uuid} = SurrealDB.run(db, "rand::uuid")
```

A "thing" passed to `select/2`, `create/3`, and friends can be:

- a bare string without a colon (`"person"`) or a `SurrealDB.Table` - a whole table.
- a string with a colon (`"person:tobie"`) or a `SurrealDB.RecordId` - one record.
- a `SurrealDB.Range` - a record id range.

## Value types

```elixir
alias SurrealDB.{RecordId, Duration, Datetime, Geometry, Range}

SurrealDB.create(db, "event", %{
  at: Datetime.from_datetime(DateTime.utc_now()),
  ttl: Duration.parse("2h30m"),
  where: Geometry.point(-0.118, 51.509),
  author: RecordId.new("person", "tobie")
})

# Record id ranges
SurrealDB.select(db, Range.new({:incl, RecordId.new("temperature", 1)}, {:excl, RecordId.new("temperature", 100)}))
```

With the CBOR protocol these types round-trip with full fidelity. With JSON they are
projected onto their closest JSON representation.

## Authentication

```elixir
# Sign in after connecting
{:ok, _token} = SurrealDB.signin(db, %SurrealDB.Auth.Root{user: "root", pass: "root"})

# Record access (scopes)
{:ok, token} =
  SurrealDB.signup(db, %SurrealDB.Auth.Record{
    namespace: "test",
    database: "app",
    access: "user",
    variables: %{email: "a@b.com", password: "secret"}
  })

# Reuse a token later
{:ok, _} = SurrealDB.authenticate(db, token)
{:ok, _} = SurrealDB.invalidate(db)
```

## Live queries

Live queries are delivered to a subscriber process as messages. They require a WebSocket
connection.

```elixir
{:ok, live_id} = SurrealDB.live(db, "person")

receive do
  {:surrealdb, :live, ^live_id, %{action: action, result: record}} ->
    IO.inspect({action, record})
end

# Send notifications to a different process, or get JSON Patch diffs
{:ok, _} = SurrealDB.live(db, "person", to: some_pid, diff: true)

SurrealDB.kill(db, live_id)
```

`action` is one of `:create`, `:update`, or `:delete`.

## Choosing the engine and protocol

```elixir
# HTTP engine, JSON protocol
{:ok, db} = SurrealDB.start_link(url: "http://localhost:8000/rpc", codec: :json)
```

The HTTP engine does not support live queries. Use a `ws`/`wss` URL for those.

## Spectron

Spectron is a typed REST service for agent memory. Create a client pinned to one context
and pass it to every call.

```elixir
client =
  SurrealDB.Spectron.new(
    endpoint: System.fetch_env!("SPECTRON_ENDPOINT"),
    context: "acme-prod",
    api_key: System.fetch_env!("SPECTRON_API_KEY")
  )

{:ok, _} = SurrealDB.Spectron.remember(client, "I just got promoted to CTO", scopes: "user/tobie")
{:ok, hits} = SurrealDB.Spectron.recall(client, "What is Tobie's role?", k: 10)
{:ok, %{"reply" => reply}} = SurrealDB.Spectron.chat(client, "What do you know about me?")
```

### Streaming chat

```elixir
{:ok, stream} = SurrealDB.Spectron.chat(client, "Tell me a story", stream: true)

for chunk <- stream do
  IO.write(chunk["delta"])
end
```

### Namespaces

Grouped operations live in dedicated modules, each taking the client first:

```elixir
{:ok, doc} = SurrealDB.Spectron.Documents.upload(client, title: "Handbook", file: "handbook.pdf")
{:ok, chunks} = SurrealDB.Spectron.Documents.chunks(client, doc["id"])

{:ok, session} = SurrealDB.Spectron.Sessions.create(client)
{:ok, _} = SurrealDB.Spectron.Session.turns(session, [%{role: "user", content: "hi"}])
{:ok, _} = SurrealDB.Spectron.Session.close(session)

{:ok, minted} = SurrealDB.Spectron.Keys.create(client, name: "ci", ttl_seconds: 3600)
```

The available namespaces are `Documents`, `Entities`, `Sessions`, `Lifecycle`, `Traces`,
`Principals`, `Scopes`, and `Keys`.

### Delegation

`on_behalf_of/2` returns a new client whose requests carry the `X-Spectron-On-Behalf-Of`
header. The original client is unchanged.

```elixir
as_alex = SurrealDB.Spectron.on_behalf_of(client, "principal:alex")
{:ok, _} = SurrealDB.Spectron.remember(as_alex, "Reviewed the Q3 plan")
```

### Errors

Spectron calls return `{:error, %SurrealDB.Spectron.Error{}}` whose `kind` is one of
`:auth`, `:validation`, `:not_found`, `:rate_limit`, `:scope`, `:server`, or `:connection`.
Each error also carries the response `trace_id` when the server provided one.

## Testing

The default test suite is fully hermetic (no network) and uses a mock engine for the
database client and [Bypass](https://hex.pm/packages/bypass) for the Spectron client:

```sh
mix test
```

Integration tests run against a real SurrealDB server and are excluded by default. Start a
server and include them:

```sh
docker run --rm -p 8000:8000 surrealdb/surrealdb:latest start --user root --pass root

mix test --include integration
```

Override the server URL with the `SURREALDB_URL` environment variable.

## License

Apache License 2.0. See [LICENSE](LICENSE).
