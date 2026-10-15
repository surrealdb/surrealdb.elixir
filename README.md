# SurrealDB for Elixir

An Elixir SDK for [SurrealDB](https://surrealdb.com) and
[Agent Memory](https://surrealdb.com/agent-memory), built for idiomatic Elixir: supervised
connections you pass around as handles, native value types, live queries delivered as
process messages, and a typed agent memory client.

It aims for feature parity with the official JavaScript SDKs (`surrealdb` and
`@surrealdb/memory`).

## Features

- **Two transport engines**, chosen automatically from the connection URL scheme:
  WebSocket (`ws`/`wss`) and HTTP (`http`/`https`).
- **Two protocols**: CBOR (default, full type fidelity) and JSON.
- **Native value types**: `RecordId`, `RecordIdRange`, `Table`, `Uuid`, `Duration`,
  `Datetime`, `Decimal`, `Geometry`, `Range`, `FileRef`, `Bytes`, `None`, and sets as
  `MapSet`.
- **Full RPC surface**: `use`, `signin`, `signup`, `authenticate`, `refresh`, `revoke`,
  `invalidate`, `reset`, `let`, `unset`, `query`, `select`, `create`, `insert`,
  `insert_relation`, `update`, `upsert`, `merge`, `patch`, `delete`, `relate`, `run`,
  `info`, `version`, plus `export`/`import`.
- **Sessions and interactive transactions** with handles you pass in place of the
  connection.
- **Live queries** delivered to any process as messages.
- **Agent Memory client** with the complete tool surface and namespaces.
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
- `:timeout` - request timeout in milliseconds, or `:infinity` (default 30000).
  `SurrealDB.query/4` takes a per-request `timeout:` too.
- `:renew_access` - renew access tokens before they expire (default `true`).
- `:version_check` - refuse servers outside `>= 2.1.0` and `< 5.0.0` (default `true`).
- `:transport_opts` - extra transport options, for example TLS settings.

`SurrealDB.health/1` checks the server's `/health` endpoint, `SurrealDB.version/1` returns
its version, and `SurrealDB.reset/1` signs out and clears the session.

### Reconnecting

A WebSocket connection reconnects automatically with exponential backoff when it drops,
and restores its session before it serves requests again: authentication, namespace and
database, `let` parameters, attached sessions, and live queries (which keep their ids).
Open transactions are not restored. Requests in flight when
it dropped fail rather than run twice; requests made while reconnecting wait and are sent
once it is back. Subscribe to status events to follow along:

```elixir
:ok = SurrealDB.subscribe_status(db)

receive do
  {:surrealdb, :status, :reconnecting, %{attempt: n, error: error}} -> ...
  {:surrealdb, :status, :connected, _info} -> ...
  {:surrealdb, :status, :disconnected, %{error: error}} -> ...  # gave up; the process stops
end
```

Tune it with `reconnect: [attempts: 10, delay: 500, max_delay: 30_000]`, or turn it off with
`reconnect: false`.

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
tobie = SurrealDB.RecordId.new("person", "tobie")
{:ok, one} = SurrealDB.select(db, tobie)

# Update, merge, patch, delete
{:ok, _} = SurrealDB.merge(db, tobie, %{active: true})
{:ok, _} = SurrealDB.patch(db, tobie, [%{"op" => "replace", "path" => "/age", "value" => 31}])
{:ok, _} = SurrealDB.delete(db, tobie)

# Parameterised queries. Each statement's result is returned in order.
{:ok, [rows]} = SurrealDB.query(db, "SELECT * FROM person WHERE age > $min", %{min: 18})

# If any statement fails, the error still carries every statement's outcome.
{:error, %SurrealDB.Error{kind: :query, details: %{results: results}}} =
  SurrealDB.query(db, "RETURN 1; THROW 'no'; RETURN 3")

# Or always get one {:ok, value} | {:error, error} per statement.
{:ok, [{:ok, 1}, {:error, _}, {:ok, 3}]} =
  SurrealDB.query_results(db, "RETURN 1; THROW 'no'; RETURN 3")

# Graph edges
article = SurrealDB.RecordId.new("article", "surreal")
{:ok, _} = SurrealDB.relate(db, tobie, "wrote", article, %{year: 2024})

# Functions
{:ok, uuid} = SurrealDB.run(db, "rand::uuid")
```

A "thing" passed to `select/2`, `create/3`, and friends can be:

- a string (`"person"`) or a `SurrealDB.Table` - a whole table. A string is always a
  table name, even when it contains a colon. (JSON cannot tell such a name from a record
  id, so over the JSON codec a table name with a colon returns an `:unsupported` error.)
- a `SurrealDB.RecordId` - one record. Parse text with `SurrealDB.RecordId.parse!/1`, or
  wrap it in a `SurrealDB.StringRecordId` to have the server parse it.
- a `SurrealDB.RecordIdRange` - the records in a range of ids. (CBOR only: over JSON it
  returns an `:unsupported` error.)

The SDK never guesses that a string is a record id, so a value like `"user:1234"` from user
input stays a string.

## Value types

```elixir
alias SurrealDB.{RecordId, RecordIdRange, Duration, Datetime, Geometry, Uuid}

SurrealDB.create(db, "event", %{
  at: Datetime.from_datetime(DateTime.utc_now()),
  ttl: Duration.parse("2h30m"),
  where: Geometry.point(-0.118, 51.509),
  author: RecordId.new("person", "tobie")
})

# Record id ranges: temperature:1..100
SurrealDB.select(db, RecordIdRange.new("temperature", {:incl, 1}, {:excl, 100}))

# Sets are MapSets, and UUIDs can be generated client side
SurrealDB.create(db, "tag", %{names: MapSet.new(["a", "b"]), ref: Uuid.v7()})
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

When a record access method is defined `WITH REFRESH`, `signin/2` and `signup/2` return
an access and refresh token pair, and the connection keeps both:

```elixir
{:ok, %{"access" => _, "refresh" => _} = pair} = SurrealDB.signin(db, record_auth)

{:ok, new_pair} = SurrealDB.refresh(db)        # exchange the refresh token for a new pair
{:ok, _} = SurrealDB.authenticate(other, pair)  # authenticate with a pair, keeping the refresh token
{:ok, _} = SurrealDB.revoke(db)                 # the refresh token can no longer be used
```

The connection renews the access token a minute before it expires (or halfway through a
shorter lifetime): with the refresh token when it has one, otherwise by signing in again
with the credentials it signed in with. Turn this off with `renew_access: false`. After a
reconnect, a refused access token is refreshed the same way.

## Sessions and transactions

A connection can carry several sessions, each with its own namespace, database,
authentication, and parameters. A session handle goes wherever a connection does:

```elixir
{:ok, tenant} = SurrealDB.new_session(db)
{:ok, _} = SurrealDB.signin(tenant, %SurrealDB.Auth.Root{user: "root", pass: "root"})
{:ok, _} = SurrealDB.use(tenant, namespace: "app", database: "tenant_a")
{:ok, rows} = SurrealDB.select(tenant, "person")

# Or start from a copy of an existing session's state.
{:ok, copy} = SurrealDB.fork_session(db)

{:ok, ids} = SurrealDB.sessions(db)
{:ok, _} = SurrealDB.close_session(tenant)
```

Interactive transactions work the same way: calls with the transaction handle run inside
it until you commit or cancel.

```elixir
{:ok, txn} = SurrealDB.begin(db)
{:ok, _} = SurrealDB.create(txn, "account", %{balance: 100})
{:ok, _} = SurrealDB.commit(txn)   # or SurrealDB.cancel(txn)

# transaction/2 commits when the function succeeds and cancels when it returns
# {:error, _} or raises.
{:ok, :moved} =
  SurrealDB.transaction(db, fn txn ->
    {:ok, _} = SurrealDB.merge(txn, from, %{balance: 90})
    {:ok, _} = SurrealDB.merge(txn, to, %{balance: 110})
    :moved
  end)
```

Sessions and transactions need a WebSocket connection and SurrealDB 3.0 or later.

## Errors

Every function returns `{:error, %SurrealDB.Error{}}` on failure. `kind` says where the
error came from (`:rpc` for the server, `:connection`, `:timeout`, `:http`, and so on). For
server errors, `server_kind` is the server's error kind and `details` its structured
details:

```elixir
case SurrealDB.select(db, "missing") do
  {:error, %SurrealDB.Error{server_kind: :not_found} = error} ->
    SurrealDB.Error.detail_kinds(error)   # ["Table"]
    SurrealDB.Error.detail_fields(error)  # %{"name" => "missing"}
end

SurrealDB.Error.token_expired?(error)
SurrealDB.Error.transaction_conflict?(error)
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

## Agent Memory

Agent Memory is a typed REST service for agent memory. Create a client pinned to one context
and pass it to every call.

```elixir
client =
  SurrealDB.Memory.new(
    endpoint: System.fetch_env!("AGENT_MEMORY_ENDPOINT"),
    context: "acme-prod",
    api_key: System.fetch_env!("AGENT_MEMORY_API_KEY")
  )

{:ok, _} = SurrealDB.Memory.remember(client, "I just got promoted to CTO", scopes: "user/tobie")
{:ok, hits} = SurrealDB.Memory.recall(client, "What is Tobie's role?", k: 10)
{:ok, %{"reply" => reply}} = SurrealDB.Memory.chat(client, "What do you know about me?")
```

### Streaming chat

```elixir
{:ok, stream} = SurrealDB.Memory.chat(client, "Tell me a story", stream: true)

for chunk <- stream do
  IO.write(chunk["delta"])
end
```

### Namespaces

Grouped operations live in dedicated modules, each taking the client first:

```elixir
{:ok, doc} = SurrealDB.Memory.Documents.upload(client, title: "Handbook", file: "handbook.pdf")
{:ok, chunks} = SurrealDB.Memory.Documents.chunks(client, doc["id"])

{:ok, session} = SurrealDB.Memory.Sessions.create(client)
{:ok, _} = SurrealDB.Memory.Session.turns(session, [%{role: "user", content: "hi"}])
{:ok, _} = SurrealDB.Memory.Session.close(session)

{:ok, minted} = SurrealDB.Memory.Keys.create(client, name: "ci", ttl_seconds: 3600)
```

The available namespaces are `Documents`, `Entities`, `Sessions`, `Lifecycle`, `Traces`,
`Principals`, `Scopes`, and `Keys`.

### Delegation

`on_behalf_of/2` returns a new client whose requests carry the `X-Spectron-On-Behalf-Of`
header. The original client is unchanged.

```elixir
as_alex = SurrealDB.Memory.on_behalf_of(client, "principal:alex")
{:ok, _} = SurrealDB.Memory.remember(as_alex, "Reviewed the Q3 plan")
```

### Errors

Agent Memory calls return `{:error, %SurrealDB.Memory.Error{}}` whose `kind` is one of
`:auth`, `:validation`, `:not_found`, `:rate_limit`, `:scope`, `:server`, or `:connection`.
Each error also carries the response `trace_id` when the server provided one.

## Testing

The default test suite is fully hermetic (no network) and uses a mock engine for the
database client and [Bypass](https://hex.pm/packages/bypass) for the Agent Memory client:

```sh
mix test
```

Integration tests run against a real SurrealDB server and are excluded by default. Start a
server and include them:

```sh
docker run --rm -p 8000:8000 surrealdb/surrealdb:v3 start --user root --pass root memory

mix test --include integration
```

Override the server URL with the `SURREALDB_URL` environment variable. CI runs the
integration tests against the latest 3.x release (`surrealdb/surrealdb:v3`) and the nightly
build (`surrealdb/surrealdb:nightly`).

## License

Apache License 2.0. See [LICENSE](LICENSE).
