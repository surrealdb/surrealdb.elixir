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
      string is always a table name, even if it contains a colon. JSON cannot tell such a
      name from a record id, so over the JSON codec it returns an `:unsupported` error.
    * a `SurrealDB.RecordId` targets a single record. Build one with
      `SurrealDB.RecordId.new/2`, or parse text with `SurrealDB.RecordId.parse!/1`.
    * a `SurrealDB.StringRecordId` targets a single record whose id the server parses.
    * a `SurrealDB.Range` targets a record id range.

  The SDK never guesses that a string is a record id: a value such as `"user:1234"` read
  from user input stays a string.
  """

  alias SurrealDB.{Connection, Error, RecordId, StringRecordId, Table, Transaction, Uuid}

  @typedoc """
  A connection handle, or a `SurrealDB.Session` or `SurrealDB.Transaction` on one. Every
  call that takes a connection also takes a session and runs in it. A transaction runs
  queries and record operations (`query`, `select`, `create`, and so on); calls that change
  session state (`use`, `signin`, `let`, ...), live queries, `run`, and `export`/`import`
  return an `:unsupported` error for one.
  """
  @type conn :: GenServer.server() | SurrealDB.Session.t() | Transaction.t()
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
    * `:reconnect` - whether a WebSocket connection reconnects and restores its session
      after it drops (default `true`). Pass a keyword list to tune the backoff:
      `attempts` (default 5, or `:infinity`), `delay` (first delay in ms, default 1000),
      `max_delay` (default 60000), `multiplier` (default 2.0), and `jitter` (default 0.1).
      See `SurrealDB.Connection` for what is restored.
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
  def close(conn), do: GenServer.stop(Connection.server(conn))

  @doc """
  Sends connection status events to `pid` (default: the caller):

      {:surrealdb, :status, :reconnecting | :connected | :disconnected, info}

  `info` holds the reconnect `:attempt` and, when there is one, the `:error`.
  """
  @spec subscribe_status(conn(), pid()) :: :ok
  def subscribe_status(conn, pid \\ self()), do: Connection.subscribe_status(conn, pid)

  @doc "Stops sending connection status events to `pid` (default: the caller)."
  @spec unsubscribe_status(conn(), pid()) :: :ok
  def unsubscribe_status(conn, pid \\ self()), do: Connection.unsubscribe_status(conn, pid)

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

  # ── Sessions and transactions ─────────────────────────────────────────────

  @doc """
  Starts a new session on the connection and returns its handle.

  A session has its own namespace, database, authentication, and parameters, and starts
  with none of them set. Pass the handle wherever a connection is expected. Sessions are
  restored after a reconnect, with their state and live queries.

  Needs a WebSocket connection and SurrealDB 3.0 or later.
  """
  @spec new_session(conn()) :: {:ok, SurrealDB.Session.t()} | {:error, Error.t()}
  def new_session(conn) do
    server = Connection.server(conn)
    session = %SurrealDB.Session{conn: server, id: Uuid.v4()}

    case Connection.call(session, "attach", []) do
      {:ok, _} -> {:ok, session}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Starts a new session with a copy of another session's state: namespace, database,
  authentication, and parameters. Changes to either afterwards do not affect the other.
  Pass the connection to fork its default session.
  """
  @spec fork_session(conn()) :: {:ok, SurrealDB.Session.t()} | {:error, Error.t()}
  def fork_session(conn) do
    source = Connection.session_info(conn)
    {_server, source_sid, _txn} = Connection.target(conn)

    with :ok <- attached(source),
         {:ok, session} <- new_session(conn),
         :ok <- copy_session(session, source, source_sid) do
      {:ok, session}
    end
  end

  defp attached(%{attached: true}), do: :ok

  defp attached(%{session: id}) do
    message = "session #{id} is not attached: it was closed, or dropped after a reconnect"
    {:error, Error.new(:connection, message)}
  end

  defp copy_session(session, source, source_sid) do
    authenticate = fn ->
      meta = %{copy_auth_from: source_sid}
      Connection.call(session, "authenticate", [source.token], meta: meta)
    end

    steps =
      [
        source.token && authenticate,
        (source.namespace || source.database) &&
          fn ->
            __MODULE__.use(session, namespace: source.namespace, database: source.database)
          end
      ] ++ Enum.map(source.vars, fn {name, value} -> fn -> let(session, name, value) end end)

    steps
    |> Enum.filter(& &1)
    |> Enum.reduce_while(:ok, fn step, :ok ->
      case step.() do
        {:ok, _} ->
          {:cont, :ok}

        {:error, error} ->
          close_session(session)
          {:halt, {:error, error}}
      end
    end)
  end

  @doc "Lists the ids of the sessions attached to the connection."
  @spec sessions(conn()) :: {:ok, [Uuid.t()]} | {:error, Error.t()}
  def sessions(conn) do
    # Over JSON the ids arrive as strings.
    case Connection.call(Connection.server(conn), "sessions", []) do
      {:ok, ids} when is_list(ids) ->
        {:ok, Enum.map(ids, fn id -> if is_binary(id), do: Uuid.new(id), else: id end)}

      other ->
        other
    end
  end

  @doc """
  Closes a session. The server kills its live queries and cancels its open transactions.
  """
  @spec close_session(SurrealDB.Session.t()) :: result()
  def close_session(%SurrealDB.Session{} = session), do: Connection.call(session, "detach", [])

  @doc """
  Begins an interactive transaction and returns its handle. Pass the handle wherever a
  connection is expected and the call runs inside the transaction, then `commit/1` or
  `cancel/1` it. `transaction/2` does that for you.

  Begin on a session handle to run the transaction in that session.

  Needs a WebSocket connection and SurrealDB 3.0 or later.
  """
  @spec begin(conn()) :: {:ok, Transaction.t()} | {:error, Error.t()}
  def begin(%Transaction{}),
    do: {:error, Error.new(:unsupported, "transactions cannot be nested")}

  def begin(conn) do
    {server, session, _txn} = Connection.target(conn)

    case Connection.call(conn, "begin", []) do
      {:ok, %Uuid{} = id} ->
        {:ok, %Transaction{conn: server, id: id, session: session && Uuid.new(session)}}

      {:ok, id} when is_binary(id) ->
        {:ok, %Transaction{conn: server, id: Uuid.new(id), session: session && Uuid.new(session)}}

      {:error, error} ->
        {:error, error}
    end
  end

  @doc "Commits a transaction."
  @spec commit(Transaction.t()) :: result()
  def commit(%Transaction{} = txn), do: Connection.call(txn_session(txn), "commit", [txn.id])

  @doc "Cancels a transaction, discarding its changes."
  @spec cancel(Transaction.t()) :: result()
  def cancel(%Transaction{} = txn), do: Connection.call(txn_session(txn), "cancel", [txn.id])

  # `commit` and `cancel` name the transaction in their params and run in its session.
  defp txn_session(%Transaction{conn: conn, session: nil}), do: conn

  defp txn_session(%Transaction{conn: conn, session: id}),
    do: %SurrealDB.Session{conn: conn, id: id}

  @doc """
  Runs `fun` in a transaction, committing it when `fun` succeeds and cancelling it when
  `fun` fails.

  `fun` receives the transaction handle. When it returns `{:error, reason}` the
  transaction is cancelled and `{:error, reason}` is returned. When it raises, throws, or
  exits, the transaction is cancelled and the exception propagates. Otherwise the
  transaction is committed and the result returned: `{:ok, value}` as is, any other value
  as `{:ok, value}`. A failed commit returns its error.

      SurrealDB.transaction(db, fn txn ->
        with {:ok, _} <- SurrealDB.merge(txn, from, %{balance: from_balance - 10}),
             {:ok, _} <- SurrealDB.merge(txn, to, %{balance: to_balance + 10}) do
          :transferred
        end
      end)
      #=> {:ok, :transferred}

  A transaction that conflicts with another fails to commit with an error for which
  `SurrealDB.Error.transaction_conflict?/1` is true, and can be retried.
  """
  @spec transaction(conn(), (Transaction.t() -> term())) :: result()
  def transaction(conn, fun) when is_function(fun, 1) do
    with {:ok, txn} <- begin(conn) do
      result =
        try do
          fun.(txn)
        catch
          kind, reason ->
            cancel_quietly(txn)
            :erlang.raise(kind, reason, __STACKTRACE__)
        end

      case result do
        {:error, _reason} = error ->
          cancel(txn)
          error

        result ->
          with {:ok, _} <- commit(txn) do
            case result do
              {:ok, _value} -> result
              value -> {:ok, value}
            end
          end
      end
    end
  end

  # Cleaning up after a failed callback must not replace its error: the connection may be
  # what failed.
  defp cancel_quietly(txn) do
    cancel(txn)
  catch
    _kind, _reason -> :ok
  end

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
  def export(conn, opts \\ [])
  def export(%Transaction{}, _opts), do: {:error, http_in_txn("export")}
  def export(conn, opts), do: SurrealDB.Rest.export(Connection.session_info(conn), opts)

  @doc """
  Imports a SurrealQL script into the selected namespace and database.

  Uses the HTTP `/import` endpoint, derived from the connection URL.
  """
  @spec import(conn(), String.t(), keyword()) :: {:ok, term()} | {:error, Error.t()}
  def import(conn, surql, opts \\ [])
  def import(%Transaction{}, _surql, _opts), do: {:error, http_in_txn("import")}

  def import(conn, surql, opts) do
    SurrealDB.Rest.import(Connection.session_info(conn), surql, opts)
  end

  # The HTTP endpoints run outside any transaction, so its writes would survive a cancel.
  defp http_in_txn(name),
    do: Error.new(:unsupported, "#{name} goes over HTTP and cannot run in a transaction")

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
