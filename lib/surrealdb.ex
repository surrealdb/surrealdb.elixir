defmodule SurrealDB do
  @moduledoc """
  Elixir client for [SurrealDB](https://surrealdb.com).

  Start a connection and pass the returned handle to every call:

      {:ok, db} =
        SurrealDB.start_link(
          url: "ws://localhost:8000/rpc",
          namespace: "test",
          database: "test",
          auth: %SurrealDB.Auth.Root{user: "root", pass: "root"}
        )

      {:ok, [tobie]} = SurrealDB.create(db, "person", %{name: "Tobie"})
      {:ok, people} = SurrealDB.select(db, "person")
      {:ok, [results]} = SurrealDB.query(db, "SELECT * FROM person WHERE age > $min", %{min: 18})

  The handle is the pid (or registered name) of a `SurrealDB.Connection` process, so it
  fits naturally in a supervision tree. The transport engine is chosen from the URL scheme
  (`ws`/`wss` use the WebSocket engine, `http`/`https` the HTTP engine) and the protocol
  defaults to CBOR for full type fidelity.

  ## Things

  Methods such as `select/2` and `create/3` accept a "thing" that identifies what to act on:

    * a string (for example `"person"`) or a `SurrealDB.Table` targets a whole table. A
      string is always a table name, even if it contains a colon.
    * a `SurrealDB.RecordId` targets a single record. Build one with
      `SurrealDB.RecordId.new/2`, or parse text with `SurrealDB.RecordId.parse!/1`.
    * a `SurrealDB.StringRecordId` targets a single record whose id the server parses.
    * a `SurrealDB.Range` targets a record id range.

  The SDK never guesses that a string is a record id: a value such as `"user:1234"` read
  from user input stays a string.
  """

  alias SurrealDB.{Connection, Error, RecordId, StringRecordId, Table, Uuid}

  @type conn :: GenServer.server()
  @type thing ::
          String.t() | Table.t() | RecordId.t() | StringRecordId.t() | SurrealDB.Range.t()
  @type record_ref :: RecordId.t() | StringRecordId.t()
  @type result :: {:ok, term()} | {:error, Error.t()}

  @doc """
  Starts a connection process and returns its handle.

  ## Options

    * `:url` (required) - connection URL, for example `"ws://localhost:8000/rpc"`.
    * `:namespace` / `:database` - selected on connect via `use`.
    * `:auth` - a `SurrealDB.Auth` struct to sign in with on connect.
    * `:codec` - `:cbor` (default) or `:json`.
    * `:name` - a name to register the process under.
    * `:connect_timeout` - milliseconds to wait for each connect-time call (`version`,
      `signin`, `use`) (default 15000).
    * `:timeout` - milliseconds before a request times out, or `:infinity` (default 30000).
      Calls that take options, such as `query/4`, can override it per request.
    * `:version_check` - when `true` (the default), the connection asks the server for its
      version and refuses to start unless it is supported: `>= 2.1.0` and `< 5.0.0`. See
      `SurrealDB.Version`.
    * `:transport_opts` - extra options passed to the transport (for example TLS settings).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  defdelegate start_link(opts), to: Connection

  @doc "Child spec so a connection can be placed directly in a supervision tree."
  defdelegate child_spec(opts), to: Connection

  @doc "Closes the connection and stops its process."
  @spec close(conn()) :: :ok
  def close(conn), do: GenServer.stop(conn)

  @doc "Returns session info: active namespace, database, engine, codec, and live support."
  @spec info_session(conn()) :: map()
  def info_session(conn), do: Connection.session_info(conn)

  # ── Session and auth ───────────────────────────────────────────────────────

  @doc """
  Selects the namespace and/or database to use.

  A key that is missing or `nil` leaves that selection unchanged, so
  `use(db, database: "other")` switches the database and keeps the namespace. Pass
  `:unset` to clear a selection. Clearing the namespace also clears the database.

      SurrealDB.use(db, namespace: "app", database: "prod")
      SurrealDB.use(db, database: "staging")
      SurrealDB.use(db, database: :unset)
  """
  @spec use(conn(), keyword()) :: result()
  def use(conn, opts) do
    params = [use_param(opts, :namespace), use_param(opts, :database)]
    Connection.call(conn, "use", params)
  end

  # The server reads NONE as "leave unchanged" and NULL as "clear".
  defp use_param(opts, key) do
    case Keyword.get(opts, key) do
      nil -> SurrealDB.None.none()
      :unset -> nil
      name when is_binary(name) -> name
    end
  end

  @doc """
  Signs in with the given credentials and returns the auth token.

  The token is a JWT string, or a map with `"access"` and `"refresh"` tokens when the
  record access method was defined `WITH REFRESH`. Either way the connection keeps the
  access token and sends it with every later request.
  """
  @spec signin(conn(), SurrealDB.Auth.t()) :: result()
  def signin(conn, auth), do: Connection.call(conn, "signin", [SurrealDB.Auth.to_params(auth)])

  @doc "Signs up via record access and returns the auth token, as `signin/2` does."
  @spec signup(conn(), SurrealDB.Auth.Record.t()) :: result()
  def signup(conn, auth), do: Connection.call(conn, "signup", [SurrealDB.Auth.to_params(auth)])

  @doc "Authenticates the connection with an existing token."
  @spec authenticate(conn(), String.t()) :: result()
  def authenticate(conn, token), do: Connection.call(conn, "authenticate", [token])

  @doc "Invalidates the current session authentication."
  @spec invalidate(conn()) :: result()
  def invalidate(conn), do: Connection.call(conn, "invalidate", [])

  @doc "Returns information about the authenticated record (the result of `$auth`)."
  @spec info(conn()) :: result()
  def info(conn), do: Connection.call(conn, "info", [])

  @doc "Returns the SurrealDB server version, for example `\"surrealdb-3.3.4\"`."
  @spec version(conn()) :: result()
  def version(conn), do: Connection.call(conn, "version", [])

  @doc """
  Checks the server's health with the HTTP `/health` endpoint, derived from the connection
  URL whatever the engine. Returns `:ok` when the server and its storage are healthy.
  """
  @spec health(conn(), keyword()) :: :ok | {:error, Error.t()}
  def health(conn, opts \\ []), do: SurrealDB.Rest.health(Connection.session_info(conn), opts)

  @doc """
  Resets the session: signs out and clears the namespace, database, and connection
  parameters. The server also kills the session's live queries and cancels its open
  transactions.
  """
  @spec reset(conn()) :: result()
  def reset(conn), do: Connection.call(conn, "reset", [])

  @doc "Defines a connection-scoped parameter usable in later queries as `$name`."
  @spec let(conn(), String.t(), term()) :: result()
  def let(conn, name, value), do: Connection.call(conn, "let", [name, value])

  @doc "Alias for `let/3`."
  @spec set(conn(), String.t(), term()) :: result()
  def set(conn, name, value), do: let(conn, name, value)

  @doc "Removes a previously defined connection-scoped parameter."
  @spec unset(conn(), String.t()) :: result()
  def unset(conn, name), do: Connection.call(conn, "unset", [name])

  # ── Queries ──────────────────────────────────────────────────────────────────

  @doc """
  Runs a SurrealQL query with optional bound variables.

  Returns `{:ok, results}` with one result per statement when every statement succeeded.

  When any statement fails it returns `{:error, %SurrealDB.Error{kind: :query}}`. The
  error's `details.results` holds every statement's outcome, in order, as `{:ok, value}`
  or `{:error, error}`, so nothing after the first failure is lost:

      case SurrealDB.query(db, "CREATE a; THROW 'no'; CREATE b") do
        {:ok, [a, _, b]} -> {a, b}
        {:error, %SurrealDB.Error{kind: :query, details: %{results: results}}} -> results
      end

  Use `query_results/3` to always get the per-statement outcomes.

  ## Options

    * `:timeout` - milliseconds before the request times out, or `:infinity`. Defaults to
      the connection's `:timeout`.
  """
  @spec query(conn(), String.t(), map(), keyword()) :: result()
  def query(conn, surql, vars \\ %{}, opts \\ []) do
    Connection.call(conn, "query", [surql, vars], call_opts(opts))
  end

  @doc """
  Runs a SurrealQL query and returns every statement's outcome, in order, as
  `{:ok, value}` or `{:error, %SurrealDB.Error{}}`. A failed statement does not hide the
  ones after it.

      {:ok, [{:ok, _}, {:error, error}, {:ok, _}]} =
        SurrealDB.query_results(db, "CREATE a; THROW 'no'; CREATE b")
  """
  @spec query_results(conn(), String.t(), map(), keyword()) ::
          {:ok, [{:ok, term()} | {:error, Error.t()}]} | {:error, Error.t()}
  def query_results(conn, surql, vars \\ %{}, opts \\ []) do
    Connection.call(conn, "query", [surql, vars], [meta: %{statements: true}] ++ call_opts(opts))
  end

  defp call_opts(opts), do: Keyword.take(opts, [:timeout])

  @doc """
  Runs a query and returns the raw per-statement objects, each with `"status"`, `"time"`,
  and `"result"`, without raising on a failed statement.
  """
  @spec query_raw(conn(), String.t(), map()) :: result()
  def query_raw(conn, surql, vars \\ %{}) do
    Connection.call(conn, "query", [surql, vars], meta: %{raw: true})
  end

  # ── CRUD ─────────────────────────────────────────────────────────────────────

  @doc "Selects all records in a table, a single record, or a record id range."
  @spec select(conn(), thing()) :: result()
  def select(conn, thing), do: Connection.call(conn, "select", [normalize_thing(thing)])

  @doc "Creates a record. `data` defaults to an empty map."
  @spec create(conn(), thing(), map()) :: result()
  def create(conn, thing, data \\ %{}) do
    Connection.call(conn, "create", [normalize_thing(thing), data])
  end

  @doc "Inserts one record (a map) or many records (a list of maps) into a table."
  @spec insert(conn(), String.t() | Table.t(), map() | [map()]) :: result()
  def insert(conn, table, data) do
    Connection.call(conn, "insert", [normalize_thing(table), data])
  end

  @doc "Inserts one or many graph edges (relations) into a table."
  @spec insert_relation(conn(), String.t() | Table.t(), map() | [map()]) :: result()
  def insert_relation(conn, table, data) do
    Connection.call(conn, "insert_relation", [normalize_thing(table), data])
  end

  @doc "Replaces the content of the matched records with `data`."
  @spec update(conn(), thing(), map()) :: result()
  def update(conn, thing, data \\ %{}) do
    Connection.call(conn, "update", [normalize_thing(thing), data])
  end

  @doc "Creates the records if they do not exist, otherwise replaces their content."
  @spec upsert(conn(), thing(), map()) :: result()
  def upsert(conn, thing, data \\ %{}) do
    Connection.call(conn, "upsert", [normalize_thing(thing), data])
  end

  @doc "Merges `data` into the matched records, keeping fields not present in `data`."
  @spec merge(conn(), thing(), map()) :: result()
  def merge(conn, thing, data) do
    Connection.call(conn, "merge", [normalize_thing(thing), data])
  end

  @doc """
  Applies a list of JSON Patch operations to the matched records.

  Pass `diff: true` to receive the applied diff instead of the full records.
  """
  @spec patch(conn(), thing(), [map()], keyword()) :: result()
  def patch(conn, thing, patches, opts \\ []) do
    params = [normalize_thing(thing), patches, Keyword.get(opts, :diff, false)]
    Connection.call(conn, "patch", params)
  end

  @doc "Deletes all records in a table, a single record, or a record id range."
  @spec delete(conn(), thing()) :: result()
  def delete(conn, thing), do: Connection.call(conn, "delete", [normalize_thing(thing)])

  @doc """
  Creates a graph edge from `from` to `to` through the `relation` table, with optional edge
  `data`.

      alias SurrealDB.RecordId
      SurrealDB.relate(db, RecordId.new("person", "tobie"), "wrote",
        RecordId.new("article", "surreal"), %{year: 2024})

  `from` and `to` must be record ids (or lists of them); a string raises `ArgumentError`.
  """
  @spec relate(
          conn(),
          record_ref() | [record_ref()],
          String.t() | Table.t(),
          record_ref() | [record_ref()],
          map()
        ) ::
          result()
  def relate(conn, from, relation, to, data \\ %{}) do
    params = [record!(from), normalize_thing(relation), record!(to), data]
    Connection.call(conn, "relate", params)
  end

  @doc "Runs a built-in or user-defined function with the given arguments."
  @spec run(conn(), String.t(), list()) :: result()
  def run(conn, name, args \\ []) when is_list(args) do
    Connection.call(conn, "run", [name, nil, args])
  end

  @doc "Runs a specific version of a user-defined function."
  @spec run(conn(), String.t(), String.t(), list()) :: result()
  def run(conn, name, version, args) when is_list(args) do
    Connection.call(conn, "run", [name, version, args])
  end

  # ── Live queries ───────────────────────────────────────────────────────────

  @doc """
  Starts a live query on a table and returns `{:ok, live_id}`.

  Notifications are delivered to the subscriber process as messages:

      {:surrealdb, :live, live_id,
       %{action: :create | :update | :delete | :killed, result: value, record: record_id}}

  `:killed` is the last message for a live query: the server ended it (for example the
  table was removed), and the connection forgets it. Subscribers are monitored; when the
  last subscriber of a live query exits, the connection kills the live query.

  ## Options

    * `:to` - the pid to deliver notifications to (default: the calling process).
    * `:diff` - when `true`, results are JSON Patch diffs instead of full records.

  Live queries require a WebSocket connection.
  """
  @spec live(conn(), String.t() | Table.t(), keyword()) :: result()
  def live(conn, table, opts \\ []) do
    subscriber = Keyword.get(opts, :to, self())
    diff = Keyword.get(opts, :diff, false)
    Connection.call(conn, "live", [normalize_thing(table), diff], meta: %{subscriber: subscriber})
  end

  @doc """
  Adds `pid` to the processes that receive notifications for an existing live query id.
  Existing subscribers keep receiving them too.
  """
  @spec subscribe_live(conn(), Uuid.t() | String.t(), pid()) :: :ok
  def subscribe_live(conn, live_id, pid \\ self()) do
    Connection.subscribe_live(conn, live_id, pid)
  end

  @doc """
  Stops delivering notifications for a live query to `pid`. The live query keeps running
  on the server; use `kill/2` to stop it.
  """
  @spec unsubscribe_live(conn(), Uuid.t() | String.t(), pid()) :: :ok
  def unsubscribe_live(conn, live_id, pid \\ self()) do
    Connection.unsubscribe_live(conn, live_id, pid)
  end

  @doc "Stops a live query."
  @spec kill(conn(), Uuid.t() | String.t()) :: result()
  def kill(conn, %Uuid{} = live_id) do
    Connection.call(conn, "kill", [live_id], meta: %{live_id: live_id})
  end

  def kill(conn, live_id) when is_binary(live_id) do
    uuid = Uuid.new(live_id)
    Connection.call(conn, "kill", [uuid], meta: %{live_id: uuid})
  end

  # ── Export and import (HTTP endpoints) ───────────────────────────────────────

  @doc """
  Exports the selected namespace and database as a SurrealQL script.

  Uses the HTTP `/export` endpoint, derived from the connection URL, regardless of the
  connection's engine.
  """
  @spec export(conn(), keyword()) :: {:ok, String.t()} | {:error, Error.t()}
  def export(conn, opts \\ []), do: SurrealDB.Rest.export(Connection.session_info(conn), opts)

  @doc """
  Imports a SurrealQL script into the selected namespace and database.

  Uses the HTTP `/import` endpoint, derived from the connection URL.
  """
  @spec import(conn(), String.t(), keyword()) :: {:ok, term()} | {:error, Error.t()}
  def import(conn, surql, opts \\ []) do
    SurrealDB.Rest.import(Connection.session_info(conn), surql, opts)
  end

  # ── Thing normalisation ──────────────────────────────────────────────────────

  @doc false
  def normalize_thing(%Table{} = table), do: table
  def normalize_thing(%RecordId{} = record), do: record
  def normalize_thing(%StringRecordId{} = record), do: record
  def normalize_thing(%SurrealDB.Range{} = range), do: range
  def normalize_thing(string) when is_binary(string), do: Table.new(string)
  def normalize_thing(other), do: other

  defp record!(%RecordId{} = record), do: record
  defp record!(%StringRecordId{} = record), do: record
  defp record!(records) when is_list(records), do: Enum.map(records, &record!/1)

  defp record!(other) do
    raise ArgumentError,
          "expected a SurrealDB.RecordId or SurrealDB.StringRecordId, got: #{inspect(other)}. " <>
            "Strings are never treated as record ids; use SurrealDB.RecordId.parse!/1."
  end
end
