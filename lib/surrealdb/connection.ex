defmodule SurrealDB.Connection do
  @moduledoc """
  The process that owns a SurrealDB connection.

  It manages the transport engine, encodes and decodes RPC messages with the chosen codec,
  correlates responses to callers, tracks the active namespace/database/token, and routes
  live query notifications to subscribers.

  You usually start it through `SurrealDB.start_link/1` and then pass the returned pid (or
  registered name) to the functions in `SurrealDB`. It can also be placed directly in a
  supervision tree:

      children = [
        {SurrealDB.Connection,
         url: "ws://localhost:8000/rpc",
         namespace: "test",
         database: "test",
         auth: %SurrealDB.Auth.Root{user: "root", pass: "root"},
         name: MyApp.SurrealDB}
      ]

  ## Reconnecting

  When a WebSocket connection drops, the connection reconnects with exponential backoff
  and restores the session before it serves requests again, in this order:

    1. the version check,
    2. authentication: the access token, or signing in again with the credentials the
       session last signed in with. If that sign-in fails, the attempt fails and the next
       one starts after the backoff,
    3. the namespace and database,
    4. connection parameters set with `SurrealDB.let/3`. If one cannot be set again, the
       attempt fails and the next one starts after the backoff,
    5. sessions started with `SurrealDB.new_session/1` or `SurrealDB.fork_session/1`:
       each is attached again under the same id, then its authentication, namespace and
       database, and parameters are restored as above. A session the server will not
       attach again is dropped with a warning, and calls on its handle fail,
    6. live queries started with `SurrealDB.live/3`, in the session that started them.
       Each one is started again on the server, and its notifications keep arriving under
       the live query id the caller already has.

  Transactions are not restored: a transaction open when the connection dropped is gone,
  and committing it fails.

  Requests in flight when the connection dropped fail with a `:connection` error rather
  than being sent twice, since the server may already have run them. Requests made while
  reconnecting wait (within their timeout) and are sent once the session is restored. If
  every attempt fails, pending requests fail and the process stops, so a supervisor can
  take over.

  Processes subscribed with `SurrealDB.subscribe_status/2` receive

      {:surrealdb, :status, status, info}

  where `status` is `:reconnecting`, `:connected`, or `:disconnected` and `info` holds the
  `:attempt` and, when there is one, the `:error`.
  """

  use GenServer

  require Logger

  alias SurrealDB.{Codec, Engine, Error, None, RPC}
  alias SurrealDB.Connection.Session

  defstruct [
    :engine,
    :engine_state,
    :engine_opts,
    :codec,
    :uri,
    :version,
    :timeout,
    :reconnect,
    version_check: true,
    connect_timeout: 15_000,
    status: :connected,
    attempt: 0,
    next_id: 1,
    pending: %{},
    # requests made while reconnecting: [{id, from, method, params, meta}], newest first
    queue: [],
    # results of synchronous setup calls, by request id
    sync: %{},
    # session id (nil for the connection's default session) => Session, in attach order
    sessions: %{nil => %Session{}},
    session_order: [],
    # live query id (as the caller knows it) => MapSet of subscriber pids
    live: %{},
    # live query id => {session id, `live` params}, to start it again after a reconnect
    live_specs: %{},
    # live query id => the id the server currently uses for it
    live_current: %{},
    # server live query id => live query id
    live_ids: %{},
    # subscriber pid => monitor ref
    monitors: %{},
    # status subscriber pid => monitor ref
    status_subscribers: %{}
  ]

  @default_timeout 30_000

  @default_reconnect [attempts: 5, delay: 1_000, max_delay: 60_000, multiplier: 2.0, jitter: 0.1]

  # The methods a transaction runs. The rest change session state (`use`, `signin`, `let`,
  # ...), start or end live queries, or run outside any transaction on the server.
  @txn_methods ~w(query select create insert insert_relation update upsert merge patch delete
                  relate gql graphql info version ping)

  @doc "Starts a connection process. See `SurrealDB.start_link/1` for the options."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @doc """
  Performs an RPC call and returns the transformed reply.

  The request times out after the connection's `:timeout` (30 seconds by default), or the
  `:timeout` given here, in milliseconds or `:infinity`. A timed-out request returns
  `{:error, %SurrealDB.Error{kind: :timeout}}`; the server may still finish it.
  """
  @spec call(handle(), String.t(), list(), keyword()) :: {:ok, term()} | {:error, Error.t()}
  def call(handle, method, params, opts \\ [])

  def call(%SurrealDB.Transaction{}, method, _params, _opts) when method not in @txn_methods,
    do: {:error, Error.new(:unsupported, "#{method} cannot run in a transaction")}

  def call(handle, method, params, opts) do
    {server, session, txn} = target(handle)
    meta = Keyword.get(opts, :meta, %{})
    meta = if opts[:timeout], do: Map.put(meta, :timeout, opts[:timeout]), else: meta
    meta = if session, do: Map.put(meta, :session, session), else: meta
    meta = if txn, do: Map.put(meta, :txn, txn), else: meta
    # The timeout runs from now, even if the connection is busy (say, reconnecting) when the
    # call reaches it.
    meta = Map.put(meta, :sent_at, System.monotonic_time(:millisecond))

    # The connection enforces the timeout and always replies, so this only waits forever
    # if the connection process itself is stuck, and the call exits if it dies.
    GenServer.call(server, {:rpc, method, params, meta}, :infinity)
  end

  @typedoc "A connection, or a session or transaction on one."
  @type handle :: GenServer.server() | SurrealDB.Session.t() | SurrealDB.Transaction.t()

  @doc false
  # The connection process, session id, and transaction id a handle refers to.
  @spec target(handle()) :: {GenServer.server(), String.t() | nil, String.t() | nil}
  def target(%SurrealDB.Session{conn: conn, id: id}), do: {conn, to_string(id), nil}

  def target(%SurrealDB.Transaction{conn: conn, id: id, session: session}),
    do: {conn, session && to_string(session), to_string(id)}

  def target(server), do: {server, nil, nil}

  @doc false
  @spec server(handle()) :: GenServer.server()
  def server(handle), do: elem(target(handle), 0)

  @doc "Adds `pid` to the subscribers of a live query id."
  @spec subscribe_live(GenServer.server(), term(), pid()) :: :ok
  def subscribe_live(handle, live_id, pid) do
    GenServer.call(server(handle), {:subscribe_live, live_id, pid})
  end

  @doc "Removes `pid` from the subscribers of a live query id."
  @spec unsubscribe_live(GenServer.server(), term(), pid()) :: :ok
  def unsubscribe_live(handle, live_id, pid) do
    GenServer.call(server(handle), {:unsubscribe_live, live_id, pid})
  end

  @doc "Sends connection status events to `pid` (see the module docs)."
  @spec subscribe_status(GenServer.server(), pid()) :: :ok
  def subscribe_status(handle, pid), do: GenServer.call(server(handle), {:subscribe_status, pid})

  @doc "Stops sending connection status events to `pid`."
  @spec unsubscribe_status(GenServer.server(), pid()) :: :ok
  def unsubscribe_status(handle, pid),
    do: GenServer.call(server(handle), {:unsubscribe_status, pid})

  @doc """
  Returns what the connection knows about the handle's session (the default session for a
  connection): namespace, database, token, parameters, plus the server version, engine,
  and status.
  """
  @spec session_info(handle()) :: map()
  def session_info(handle) do
    {server, session, _txn} = target(handle)
    GenServer.call(server, {:session_info, session})
  end

  # ── GenServer callbacks ─────────────────────────────────────────────────

  @impl true
  def init(opts) do
    url = Keyword.fetch!(opts, :url)
    uri = URI.parse(url)
    codec = Codec.resolve(Keyword.get(opts, :codec, :cbor))

    with {:ok, engine} <- resolve_engine(opts, uri),
         engine_opts = engine_opts(codec, opts),
         {:ok, engine_state} <- engine.connect(uri, engine_opts) do
      auth = Keyword.get(opts, :auth)

      state = %__MODULE__{
        engine: engine,
        engine_state: engine_state,
        engine_opts: engine_opts,
        codec: codec,
        uri: uri,
        sessions: %{
          nil => %Session{
            namespace: Keyword.get(opts, :namespace),
            database: Keyword.get(opts, :database),
            credentials: auth && SurrealDB.Auth.to_params(auth)
          }
        },
        timeout: Keyword.get(opts, :timeout, @default_timeout),
        version_check: Keyword.get(opts, :version_check, true),
        connect_timeout: Keyword.get(opts, :connect_timeout, 15_000),
        reconnect: reconnect_config(engine, Keyword.get(opts, :reconnect, true))
      }

      setup(state)
    else
      {:error, reason} ->
        {:stop, Error.new(:connection, "could not connect: #{inspect(reason)}", details: reason)}
    end
  end

  # Sign in before selecting the namespace and database: the server only creates a missing
  # namespace or database for a caller allowed to, so a `use` sent before `signin` leaves a
  # fresh database undefined and every later query fails.
  defp setup(state) do
    with {:ok, state} <- check_version(state),
         {:ok, state} <- connect_signin(state),
         {:ok, state} <- restore_use(state, nil) do
      {:ok, state}
    else
      {:error, reason, _state} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:rpc, method, params, meta}, from, state) do
    cond do
      needs_sessions?(method, meta) and not state.engine.supports_live?() ->
        message = "sessions and transactions need a WebSocket connection"
        {:reply, {:error, Error.new(:unsupported, message)}, state}

      error = json_ambiguous_table(state, params) ->
        {:reply, {:error, error}, state}

      true ->
        meta = local_sent_at(meta, from)
        timeout = Map.get(meta, :timeout, state.timeout)

        # A call that waited out its whole timeout in the mailbox (behind a reconnect, say)
        # is answered here: its caller has given up, so it must not be sent.
        if out_of_time?(meta, timeout) do
          {:reply, timeout_error(method), state}
        else
          {id, state} = next_id(state)
          dispatch(state, id, from, method, params, start_timer(meta, id, timeout))
        end
    end
  end

  def handle_call({:subscribe_live, live_id, pid}, _from, state) do
    {:reply, :ok, add_subscriber(state, normalize_id(live_id), pid)}
  end

  def handle_call({:unsubscribe_live, live_id, pid}, _from, state) do
    {:reply, :ok, remove_subscriber(state, normalize_id(live_id), pid)}
  end

  def handle_call({:subscribe_status, pid}, _from, state) do
    subscribers = Map.put_new_lazy(state.status_subscribers, pid, fn -> Process.monitor(pid) end)
    {:reply, :ok, %{state | status_subscribers: subscribers}}
  end

  def handle_call({:unsubscribe_status, pid}, _from, state) do
    {ref, subscribers} = Map.pop(state.status_subscribers, pid)
    if ref, do: Process.demonitor(ref, [:flush])
    {:reply, :ok, %{state | status_subscribers: subscribers}}
  end

  def handle_call({:session_info, sid}, _from, state) do
    session = Map.get(state.sessions, sid, %Session{})

    info = %{
      session: sid && SurrealDB.Uuid.new(sid),
      # false for a session that was closed, or dropped after a reconnect
      attached: Map.has_key?(state.sessions, sid),
      namespace: session.namespace,
      database: session.database,
      token: session.token,
      refresh_token: session.refresh_token,
      vars: session.vars,
      authenticated: not is_nil(session.token),
      version: state.version,
      status: state.status,
      uri: state.uri,
      transport_opts: Keyword.get(state.engine_opts, :transport_opts),
      engine: state.engine,
      codec: state.codec,
      supports_live?: state.engine.supports_live?()
    }

    {:reply, info, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason} = message, state) do
    cond do
      Map.get(state.monitors, pid) == ref ->
        {:noreply, subscriber_down(state, pid)}

      Map.get(state.status_subscribers, pid) == ref ->
        {:noreply, %{state | status_subscribers: Map.delete(state.status_subscribers, pid)}}

      true ->
        handle_transport(message, state)
    end
  end

  def handle_info({:request_timeout, id}, state) do
    case Map.pop(state.pending, id) do
      {{from, method, params, meta}, pending} ->
        state = reply(%{state | pending: pending}, from, id, timeout_error(method))

        state = %{
          state
          | pending: await_late_reply(state, state.pending, id, method, params, meta)
        }

        cancel_in_engine(state, id)

      {nil, _pending} ->
        case List.keytake(state.queue, id, 0) do
          {{^id, from, method, _params, _meta}, queue} ->
            {:noreply, reply(%{state | queue: queue}, from, id, timeout_error(method))}

          nil ->
            {:noreply, state}
        end
    end
  end

  def handle_info({:reconnect, attempt}, %{status: :reconnecting} = state) do
    {:noreply, reconnect(state, attempt)}
    |> maybe_stop()
  end

  def handle_info({:reconnect, _attempt}, state), do: {:noreply, state}

  def handle_info(message, %{engine_state: nil} = state) do
    Logger.debug("SurrealDB ignored a message while disconnected: #{inspect(message)}")
    {:noreply, state}
  end

  def handle_info(message, state), do: handle_transport(message, state)

  # Requests made while reconnecting wait in the queue until the session is restored.
  defp dispatch(%{status: :reconnecting} = state, id, from, method, params, meta),
    do: {:noreply, %{state | queue: [{id, from, method, params, meta} | state.queue]}}

  defp dispatch(state, id, from, method, params, meta) do
    case send_request(state, id, from, method, params, meta) do
      {:ok, state} -> {:noreply, state}
      {:error, error, state} -> {:reply, {:error, error}, state}
    end
  end

  # Let the engine clean up after a timed-out request (see `Engine.cancel/2`).
  defp cancel_in_engine(state, id) do
    if function_exported?(state.engine, :cancel, 2) do
      {:ok, events, engine_state} = state.engine.cancel(state.engine_state, id)
      state = %{state | engine_state: engine_state}

      case Enum.reduce_while(events, {:ok, state}, &process_event/2) do
        {:ok, state} -> {:noreply, state}
        {:closed, state} -> connection_lost(state, "connection closed")
      end
    else
      {:noreply, state}
    end
  end

  # Methods whose reply changes what the connection tracks about the session.
  @session_methods ~w(use signin signup authenticate invalidate reset let set unset)

  # A timed-out request on an engine that cannot cancel it (a WebSocket) may still run on
  # the server. If it changes session state, keep it pending with no caller, so its late
  # reply still updates the namespace, database, and auth the connection reports and sends.
  # A late `live` reply starts a query nobody listens to, so it is killed; a late `attach`
  # or `begin` opens a session or transaction nobody holds, so it is detached or cancelled.
  # Any other request is forgotten: its late reply carries an id, so it is never read as a
  # notification.
  defp await_late_reply(state, pending, id, method, params, meta) do
    cond do
      function_exported?(state.engine, :cancel, 2) ->
        pending

      method == "live" ->
        meta = meta |> Map.take([:session]) |> Map.put(:late_live, true)
        Map.put(pending, id, {nil, method, params, meta})

      method in @session_methods ->
        Map.put(pending, id, {nil, method, params, Map.delete(meta, :timer)})

      method in ["attach", "begin"] ->
        meta = meta |> Map.take([:session]) |> Map.put(:late, true)
        Map.put(pending, id, {nil, method, params, meta})

      true ->
        pending
    end
  end

  defp handle_transport(message, state) do
    case state.engine.handle_transport(state.engine_state, message) do
      :unknown ->
        {:noreply, state}

      {:ok, events, engine_state} ->
        state = %{state | engine_state: engine_state}

        case Enum.reduce_while(events, {:ok, state}, &process_event/2) do
          {:ok, state} -> {:noreply, state}
          {:closed, state} -> connection_lost(state, "connection closed")
        end

      {:error, reason, engine_state} ->
        connection_lost(%{state | engine_state: engine_state}, reason)
    end
  end

  @impl true
  def terminate(_reason, state) do
    if state.engine_state, do: state.engine.close(state.engine_state)
    :ok
  end

  # ── Losing the connection and reconnecting ─────────────────────────────────

  defp connection_lost(state, reason) do
    error = conn_error(reason)
    Logger.warning("SurrealDB connection lost: #{Exception.message(error)}")
    state = state |> fail_pending(error) |> close_engine()

    if state.reconnect do
      state = %{state | status: :reconnecting, attempt: 0}
      emit_status(state, :reconnecting, %{attempt: 1, error: error})
      schedule_reconnect(state, 1)
      {:noreply, state}
    else
      # The transport is gone for good. Fail every caller and stop, so a supervisor can
      # start a fresh connection instead of this process lingering with a dead socket.
      {:stop, {:shutdown, {:connection_lost, error}}, state}
    end
  end

  defp reconnect(state, attempt) do
    state = %{state | attempt: attempt}

    result =
      case state.engine.connect(state.uri, state.engine_opts) do
        {:ok, engine_state} -> restore(%{state | engine_state: engine_state})
        {:error, reason} -> {:retry, conn_error(reason), state}
      end

    case result do
      {:ok, state} ->
        Logger.info("SurrealDB reconnected after #{attempt} attempt(s)")
        state = %{state | status: :connected, attempt: 0}
        emit_status(state, :connected, %{attempt: attempt, version: state.version})
        flush_queue(state)

      {:retry, error, state} ->
        state = close_engine(state)

        if attempt >= state.reconnect[:attempts] do
          give_up(state, error)
        else
          emit_status(state, :reconnecting, %{attempt: attempt + 1, error: error})
          schedule_reconnect(state, attempt + 1)
          state
        end

      {:fatal, error, state} ->
        give_up(close_engine(state), error)
    end
  end

  defp give_up(state, error) do
    Logger.error("SurrealDB could not reconnect: #{Exception.message(error)}")
    emit_status(state, :disconnected, %{attempt: state.attempt, error: error})
    state = fail_queue(state, error)
    %{state | status: {:stop, error}}
  end

  defp maybe_stop({:noreply, %{status: {:stop, error}} = state}),
    do: {:stop, {:shutdown, {:connection_lost, error}}, %{state | status: :disconnected}}

  defp maybe_stop(other), do: other

  # Restores everything the session had before the connection dropped. A transport failure
  # is retried; an unsupported server is not.
  defp restore(state) do
    with {:ok, state} <- check_version(state),
         {:ok, state} <- restore_session(state, nil),
         {:ok, state} <- restore_attached(state),
         {:ok, state} <- restore_live(state) do
      {:ok, state}
    else
      {:error, %Error{kind: :unsupported} = error, state} -> {:fatal, error, state}
      {:error, error, state} -> {:retry, error, state}
    end
  end

  defp schedule_reconnect(state, attempt) do
    Process.send_after(self(), {:reconnect, attempt}, backoff(state.reconnect, attempt))
  end

  @doc false
  # The delay before reconnect attempt `attempt` (1-based): `delay * multiplier^(attempt-1)`,
  # spread by up to `jitter` either way and capped at `max_delay`.
  def backoff(config, attempt) do
    base = config[:delay] * :math.pow(config[:multiplier], exponent(config, attempt))
    spread = 1 + config[:jitter] * (2 * :rand.uniform() - 1)
    round(min(min(base, config[:max_delay]) * spread, config[:max_delay]))
  end

  # The exponent stops growing once the delay has reached `max_delay`, so an unlimited
  # number of attempts can't overflow `:math.pow/2`.
  defp exponent(config, attempt) do
    delay = config[:delay]
    multiplier = config[:multiplier]
    max_delay = config[:max_delay]

    cond do
      multiplier <= 1 -> attempt - 1
      delay <= 0 or max_delay <= delay -> 0
      true -> min(attempt - 1, ceil(:math.log(max_delay / delay) / :math.log(multiplier)))
    end
  end

  defp flush_queue(state) do
    state.queue
    |> Enum.reverse()
    |> Enum.reduce(%{state | queue: []}, fn {id, from, method, params, meta}, state ->
      if expired?(meta) do
        # Its timeout message is still in the mailbox; answer now and don't send it late.
        reply(state, from, id, timeout_error(method))
      else
        case send_request(state, id, from, method, params, meta) do
          {:ok, state} ->
            state

          {:error, error, state} ->
            cancel_timer(meta)
            reply(state, from, id, {:error, error})
        end
      end
    end)
  end

  defp expired?(%{timer: timer}), do: Process.read_timer(timer) == false
  defp expired?(_meta), do: false

  defp fail_queue(state, error) do
    Enum.reduce(state.queue, %{state | queue: []}, fn {id, from, _method, _params, meta}, state ->
      cancel_timer(meta)
      reply(state, from, id, {:error, error})
    end)
  end

  defp close_engine(%{engine_state: nil} = state), do: state

  defp close_engine(state) do
    state.engine.close(state.engine_state)
    %{state | engine_state: nil}
  end

  defp emit_status(state, status, info) do
    Enum.each(state.status_subscribers, fn {pid, _ref} ->
      send(pid, {:surrealdb, :status, status, info})
    end)
  end

  defp reconnect_config(engine, setting) do
    cond do
      # Only engines that hold a connection lose session state; HTTP reopens on demand.
      not engine.supports_live?() -> nil
      setting == false -> nil
      setting == true -> @default_reconnect
      is_list(setting) -> Keyword.merge(@default_reconnect, setting)
    end
  end

  # ── Session setup and replay ─────────────────────────────────────────────

  # Ask the server for its version first, and refuse servers outside the supported range.
  defp check_version(state) do
    case rpc_sync(state, "version", []) do
      {:ok, version, state} when is_binary(version) ->
        state = %{state | version: version}

        case state.version_check && SurrealDB.Version.check(version) do
          {:error, error} -> {:error, error, state}
          _ok -> {:ok, state}
        end

      {:ok, other, state} ->
        if state.version_check do
          message =
            "the server's version reply #{inspect(other)} is not a version; " <>
              "pass `version_check: false` to connect anyway"

          {:error, Error.new(:unsupported, message, details: other), state}
        else
          {:ok, state}
        end

      # A dead transport fails startup whatever the check setting; with the check off,
      # only a server that cannot answer `version` is let through.
      {:error, %Error{kind: kind} = error, state} when kind in [:connection, :timeout] ->
        {:error, error, state}

      {:error, error, state} ->
        if state.version_check, do: {:error, error, state}, else: {:ok, state}
    end
  end

  defp connect_signin(state) do
    case session(state, nil).credentials do
      nil ->
        {:ok, state}

      credentials ->
        case rpc_sync(state, "signin", [credentials]) do
          {:ok, _result, state} -> {:ok, state}
          {:error, reason, state} -> {:error, reason, state}
        end
    end
  end

  defp restore_session(state, sid) do
    with {:ok, state} <- restore_auth(state, sid),
         {:ok, state} <- restore_use(state, sid) do
      restore_vars(state, sid)
    end
  end

  # Sessions live on one connection, so after a reconnect each attached session is
  # attached again under its id before its state is restored.
  defp restore_attached(state) do
    Enum.reduce_while(state.session_order, {:ok, state}, fn sid, {:ok, state} ->
      result =
        case rpc_sync(state, "attach", [], sid) do
          {:ok, _result, state} -> restore_session(state, sid) |> detach_on_error(sid)
          {:error, error, state} -> {:error, error, state}
        end

      case result do
        {:ok, state} ->
          {:cont, {:ok, state}}

        # A slow server is not a refusal; the reconnect attempt is tried again.
        {:error, %Error{kind: kind} = error, state} when kind in [:connection, :timeout] ->
          {:halt, {:error, error, state}}

        {:error, error, state} ->
          Logger.warning("SurrealDB could not restore session #{sid}: #{error.message}")
          {:cont, {:ok, forget_session(state, sid)}}
      end
    end)
  end

  # A session that was attached again but could not be restored is dropped. Detach it
  # (without waiting) so it does not stay attached on the server, out of reach.
  defp detach_on_error({:error, %Error{kind: kind} = error, state}, sid)
       when kind not in [:connection, :timeout] do
    {id, state} = next_id(state)

    case send_request(state, id, nil, "detach", [], %{session: sid}) do
      {:ok, state} -> {:error, error, state}
      {:error, _error, state} -> {:error, error, state}
    end
  end

  defp detach_on_error(result, _sid), do: result

  # Use the token if the server still accepts it, otherwise sign in again with the stored
  # credentials. A refused token with no credentials to fall back on, or a failed sign-in,
  # fails the attempt: serving queued requests signed out would run them with the wrong
  # permissions.
  defp restore_auth(state, sid) do
    case session(state, sid) do
      %Session{token: nil, credentials: nil} -> {:ok, state}
      %Session{token: nil} -> restore_signin(state, sid)
      %Session{} = session -> restore_token(state, sid, session)
    end
  end

  defp restore_token(state, sid, %Session{token: token} = session) do
    %Session{refresh_token: refresh, credentials: credentials} = session

    case rpc_sync(state, "authenticate", [token], sid) do
      {:ok, _result, state} ->
        {:ok,
         update_session(state, sid, &%{&1 | refresh_token: refresh, credentials: credentials})}

      {:error, %Error{kind: kind} = error, state} when kind in [:connection, :timeout] ->
        {:error, error, state}

      {:error, refused, state} when is_nil(credentials) ->
        {:error, refused, state}

      {:error, _unusable, state} ->
        state =
          update_session(
            state,
            sid,
            &%{&1 | token: nil, refresh_token: nil, credentials: credentials}
          )

        restore_signin(state, sid)
    end
  end

  # The attempt fails and the next one tries again.
  defp restore_signin(state, sid) do
    case rpc_sync(state, "signin", [session(state, sid).credentials], sid) do
      {:ok, _result, state} -> {:ok, state}
      {:error, error, state} -> {:error, error, state}
    end
  end

  defp restore_use(state, sid) do
    case session(state, sid) do
      %Session{namespace: nil, database: nil} ->
        {:ok, state}

      %Session{namespace: namespace, database: database} ->
        params = [namespace || None.none(), database || None.none()]

        case rpc_sync(state, "use", params, sid) do
          {:ok, _result, state} -> {:ok, state}
          {:error, reason, state} -> {:error, reason, state}
        end
    end
  end

  defp restore_vars(state, sid) do
    Enum.reduce_while(session(state, sid).vars, {:ok, state}, fn {name, value}, {:ok, state} ->
      case rpc_sync(state, "let", [name, value], sid) do
        {:ok, _result, state} -> {:cont, {:ok, state}}
        # Carrying on would serve requests that expect the parameter without it.
        {:error, error, state} -> {:halt, {:error, error, state}}
      end
    end)
  end

  # Starts every managed live query again and points its caller-facing id at the new
  # server id (see `register_live/5`). A live query the server refuses to restart is ended
  # with `:killed`.
  defp restore_live(state) do
    state = %{state | live_ids: %{}}

    Enum.reduce_while(state.live_specs, {:ok, state}, fn {id, {sid, params}}, {:ok, state} ->
      case rpc_sync(state, "live", params, sid, %{restore_live: id}) do
        {:ok, _server_id, state} ->
          {:cont, {:ok, state}}

        # A slow reply is not a refusal: the server may have started it. Try again.
        {:error, %Error{kind: kind} = error, state} when kind in [:connection, :timeout] ->
          {:halt, {:error, error, state}}

        {:error, error, state} ->
          Logger.warning("SurrealDB could not restart live query #{id}: #{error.message}")
          message = %{action: :killed, result: nil, record: nil, error: error}
          notify_live(state, id, message)
          {:cont, {:ok, drop_live(state, id)}}
      end
    end)
  end

  # ── Synchronous calls ───────────────────────────────────────────────────

  # Sends a request and waits for its reply inside the connection process, for setup and
  # replay. The reply goes through the normal routing (so notifications and other replies
  # keep flowing), and any message that is not for the transport, such as a caller's
  # `GenServer.call`, is put back in the mailbox afterwards rather than lost.
  defp rpc_sync(state, method, params, sid \\ nil, meta \\ %{}) do
    {id, state} = next_id(state)

    case send_request(state, id, :sync, method, params, Map.put(meta, :session, sid)) do
      {:ok, state} ->
        deadline = System.monotonic_time(:millisecond) + state.connect_timeout
        await_sync(state, id, method, deadline, [])

      {:error, error, state} ->
        {:error, error, state}
    end
  end

  defp await_sync(state, id, method, deadline, stash) do
    case Map.pop(state.sync, id) do
      {{:ok, value}, sync} ->
        finish_sync({:ok, value, %{state | sync: sync}}, stash)

      {{:error, error}, sync} ->
        finish_sync({:error, error, %{state | sync: sync}}, stash)

      {nil, _sync} ->
        remaining = max(deadline - System.monotonic_time(:millisecond), 0)

        receive do
          message -> await_sync_message(state, id, method, deadline, stash, message)
        after
          remaining ->
            state = %{state | pending: Map.delete(state.pending, id)}
            error = Error.new(:timeout, "RPC #{method} timed out while connecting")
            finish_sync({:error, error, state}, stash)
        end
    end
  end

  defp await_sync_message(state, id, method, deadline, stash, message) do
    case state.engine.handle_transport(state.engine_state, message) do
      :unknown ->
        case absorb(state, message) do
          {:ok, state} -> await_sync(state, id, method, deadline, stash)
          :stash -> await_sync(state, id, method, deadline, [message | stash])
        end

      {:ok, events, engine_state} ->
        case Enum.reduce_while(
               events,
               {:ok, %{state | engine_state: engine_state}},
               &process_event/2
             ) do
          {:ok, state} ->
            await_sync(state, id, method, deadline, stash)

          {:closed, state} ->
            error = Error.new(:connection, "connection closed")
            finish_sync({:error, error, fail_sync(state, id, error)}, stash)
        end

      {:error, reason, engine_state} ->
        error = conn_error(reason)

        finish_sync(
          {:error, error, fail_sync(%{state | engine_state: engine_state}, id, error)},
          stash
        )
    end
  end

  # Failing the pending requests also files this one's result under `sync`; nothing will
  # read it, so drop it rather than keep one per failed attempt.
  defp fail_sync(state, id, error) do
    state = fail_pending(state, error)
    %{state | sync: Map.delete(state.sync, id)}
  end

  # While a reconnect restores the session, calls join the queue at once (so their timers
  # start) and queued calls time out on time, rather than waiting in the stash.
  defp absorb(%{status: :reconnecting} = state, {:"$gen_call", from, {:rpc, _, _, _} = call}) do
    case handle_call(call, from, state) do
      {:noreply, state} ->
        {:ok, state}

      {:reply, reply, state} ->
        GenServer.reply(from, reply)
        {:ok, state}
    end
  end

  defp absorb(%{status: :reconnecting} = state, {:request_timeout, id}) do
    case List.keytake(state.queue, id, 0) do
      {{^id, from, method, _params, _meta}, queue} ->
        {:ok, reply(%{state | queue: queue}, from, id, timeout_error(method))}

      nil ->
        :stash
    end
  end

  defp absorb(_state, _message), do: :stash

  defp finish_sync(result, stash) do
    stash |> Enum.reverse() |> Enum.each(&send(self(), &1))
    result
  end

  # ── Event processing ────────────────────────────────────────────────────

  defp process_event(:closed, {:ok, state}), do: {:halt, {:closed, state}}

  defp process_event({:reset, reason}, {:ok, state}),
    do: {:cont, {:ok, fail_pending(state, reason)}}

  defp process_event({:reply, id, binary}, {:ok, state}) do
    case decode(state, binary) do
      {:ok, %{} = decoded} ->
        # The transport knows which request this answers; the body's id is not trusted.
        {:cont, {:ok, route(Map.put(decoded, "id", id), state)}}

      _other ->
        error = Error.new(:decode, "could not decode the reply to request #{id}", details: binary)
        {:cont, {:ok, deliver_result(id, {:error, error}, state)}}
    end
  end

  defp process_event({:failed, id, error}, {:ok, state}),
    do: {:cont, {:ok, deliver_result(id, {:error, error}, state)}}

  defp process_event({:message, binary}, {:ok, state}) do
    case decode(state, binary) do
      {:ok, %{} = decoded} ->
        {:cont, {:ok, route(decoded, state)}}

      other ->
        Logger.warning("SurrealDB could not decode message: #{inspect(other)}")
        {:cont, {:ok, state}}
    end
  end

  defp route(decoded, state) do
    case notification(decoded, state) do
      {:notification, server_id, notification} -> dispatch_live(state, server_id, notification)
      :reply -> deliver_reply(decoded, state)
    end
  end

  defp deliver_reply(decoded, state), do: deliver_result(decoded["id"], to_result(decoded), state)

  defp deliver_result(id, result, state) do
    case Map.pop(state.pending, id) do
      {nil, _pending} ->
        state

      {{nil, method, _params, %{late: true} = meta}, pending} ->
        undo_late(%{state | pending: pending}, method, Map.get(meta, :session), result)

      {{from, method, params, meta}, pending} ->
        cancel_timer(meta)
        state = %{state | pending: pending}
        sid = Map.get(meta, :session)
        state = apply_effects(state, sid, method, params, result)
        state = copy_auth(state, sid, method, meta, result)
        state = register_live(state, method, params, meta, result)
        reply(state, from, id, public_reply(method, meta, result))
    end
  end

  # A reply carries the id of a pending request. Anything else whose result looks like a
  # notification is one: SurrealDB 2.x put the notification fields at the top level, 3.x
  # sends them under `result` with no top-level id. Checking for a pending id first means a
  # reply whose result happens to have an `action` field is still delivered as a reply, and
  # a message with an id nobody is waiting for is never read as a 3.x notification.
  defp notification(decoded, state) do
    cond do
      Map.has_key?(state.pending, decoded["id"]) ->
        :reply

      Map.has_key?(decoded, "action") ->
        {:notification, decoded["id"], decoded}

      is_nil(decoded["id"]) and is_map(decoded["result"]) and
          Map.has_key?(decoded["result"], "action") ->
        {:notification, decoded["result"]["id"], decoded["result"]}

      true ->
        :reply
    end
  end

  # ── Live queries ───────────────────────────────────────────────────────────

  defp dispatch_live(state, server_id, notification) do
    server_id = normalize_id(server_id)
    id = Map.get(state.live_ids, server_id, server_id)
    action = RPC.action(notification["action"])
    message = %{action: action, result: notification["result"], record: notification["record"]}
    notify_live(state, id, message)

    # KILLED is the last notification a live query sends: the server ended it (KILL, a
    # removed table, a revoked principal), so forget it and its subscribers.
    if action == :killed, do: drop_live(state, id), else: state
  end

  defp notify_live(state, id, message) do
    public_id = SurrealDB.Uuid.new(id)

    state.live
    |> Map.get(id, MapSet.new())
    |> Enum.each(&send(&1, {:surrealdb, :live, public_id, message}))
  end

  defp register_live(state, "live", _params, %{late_live: true} = meta, {:ok, server_id}),
    do: kill_orphan(state, normalize_id(server_id), Map.get(meta, :session))

  defp register_live(state, "live", params, %{subscriber: pid} = meta, {:ok, server_id})
       when is_pid(pid) do
    id = normalize_id(server_id)

    state = %{
      state
      | live_specs: Map.put(state.live_specs, id, {Map.get(meta, :session), params}),
        live_current: Map.put(state.live_current, id, id),
        live_ids: Map.put(state.live_ids, id, id)
    }

    add_subscriber(state, id, pid)
  end

  # A live query restarted after a reconnect. The mapping goes in as the reply is handled,
  # so a notification in the same transport batch already reaches its subscribers.
  defp register_live(state, "live", _params, %{restore_live: id}, {:ok, server_id}) do
    server_id = normalize_id(server_id)

    %{
      state
      | live_current: Map.put(state.live_current, id, server_id),
        live_ids: Map.put(state.live_ids, server_id, id)
    }
  end

  defp register_live(state, "kill", _params, %{live_id: live_id}, {:ok, _result}) do
    drop_live(state, normalize_id(live_id))
  end

  defp register_live(state, _method, _params, _meta, _result), do: state

  # A caller kills a live query by the id it was given; the server may know it by a newer
  # id after a reconnect.
  defp server_params(state, "kill", [live_id], meta) do
    id = normalize_id(live_id)

    case Map.fetch(state.live_current, id) do
      {:ok, server_id} -> {[SurrealDB.Uuid.new(server_id)], Map.put(meta, :live_id, id)}
      :error -> {[live_id], meta}
    end
  end

  defp server_params(_state, _method, params, meta), do: {params, meta}

  defp add_subscriber(state, id, pid) do
    subscribers = state.live |> Map.get(id, MapSet.new()) |> MapSet.put(pid)
    state = %{state | live: Map.put(state.live, id, subscribers)}

    if Map.has_key?(state.monitors, pid) do
      state
    else
      %{state | monitors: Map.put(state.monitors, pid, Process.monitor(pid))}
    end
  end

  defp remove_subscriber(state, id, pid) do
    case Map.fetch(state.live, id) do
      {:ok, subscribers} ->
        subscribers = MapSet.delete(subscribers, pid)
        state = %{state | live: Map.put(state.live, id, subscribers)}
        demonitor_unused(state, [pid])

      :error ->
        state
    end
  end

  defp drop_live(state, id) do
    {subscribers, live} = Map.pop(state.live, id, MapSet.new())
    {server_id, live_current} = Map.pop(state.live_current, id)

    state = %{
      state
      | live: live,
        live_specs: Map.delete(state.live_specs, id),
        live_current: live_current,
        live_ids: Map.delete(state.live_ids, server_id || id)
    }

    demonitor_unused(state, MapSet.to_list(subscribers))
  end

  # A subscriber exited. Drop it everywhere, and kill every live query it was the last
  # subscriber of, so the server stops sending notifications nobody will read.
  defp subscriber_down(state, pid) do
    state = %{state | monitors: Map.delete(state.monitors, pid)}

    Enum.reduce(state.live, state, fn {id, subscribers}, state ->
      if MapSet.member?(subscribers, pid) do
        remaining = MapSet.delete(subscribers, pid)

        if MapSet.size(remaining) == 0 do
          server_id = Map.get(state.live_current, id, id)
          {sid, _params} = Map.get(state.live_specs, id, {nil, nil})
          state |> drop_live(id) |> kill_orphan(server_id, sid)
        else
          %{state | live: Map.put(state.live, id, remaining)}
        end
      else
        state
      end
    end)
  end

  # The live query belongs to a session, and only that session can kill it.
  defp kill_orphan(state, server_id, sid),
    do: send_orphan(state, "kill", [SurrealDB.Uuid.new(server_id)], sid)

  # A session or transaction that opened after its caller gave up is closed again.
  defp undo_late(state, "attach", sid, {:ok, _result}), do: send_orphan(state, "detach", [], sid)
  defp undo_late(state, "begin", sid, {:ok, txn}), do: send_orphan(state, "cancel", [txn], sid)
  defp undo_late(state, _method, _sid, _result), do: state

  # Best-effort cleanup that nobody waits for. A reconnect discards the server's state anyway.
  defp send_orphan(%{status: :reconnecting} = state, _method, _params, _sid), do: state

  defp send_orphan(state, method, params, sid) do
    {id, state} = next_id(state)

    case send_request(state, id, nil, method, params, %{session: sid}) do
      {:ok, state} -> state
      {:error, _error, state} -> state
    end
  end

  defp demonitor_unused(state, pids) do
    Enum.reduce(pids, state, fn pid, state ->
      still_used = Enum.any?(state.live, fn {_id, subs} -> MapSet.member?(subs, pid) end)

      case {still_used, Map.pop(state.monitors, pid)} do
        {false, {ref, monitors}} when is_reference(ref) ->
          Process.demonitor(ref, [:flush])
          %{state | monitors: monitors}

        _other ->
          state
      end
    end)
  end

  # ── Result shaping ─────────────────────────────────────────────────────────

  defp to_result(%{"error" => error}), do: {:error, Error.from_rpc(error)}
  defp to_result(%{"result" => result}), do: {:ok, result}
  defp to_result(_decoded), do: {:ok, nil}

  defp public_reply(_method, _meta, {:error, _error} = error), do: error

  defp public_reply("query", %{raw: true}, {:ok, statements}), do: {:ok, statements}

  defp public_reply("query", %{statements: true}, {:ok, statements}) when is_list(statements),
    do: {:ok, RPC.statement_results(statements)}

  defp public_reply("query", _meta, {:ok, statements}) when is_list(statements),
    do: RPC.parse_query(statements)

  # Over JSON the server sends a live query id as a string. Return it as the `Uuid` the
  # notifications carry, so a caller can match one against the other.
  defp public_reply("live", _meta, {:ok, id}) when is_binary(id),
    do: {:ok, SurrealDB.Uuid.new(id)}

  defp public_reply(_method, _meta, {:ok, value}), do: {:ok, value}

  # ── State mutation from method side effects ─────────────────────────────────

  # A fork authenticates with its source's token and takes the source's refresh token and
  # credentials in the same step, so it can sign in again on its own if a reconnect comes
  # straight after.
  defp copy_auth(state, sid, "authenticate", %{copy_auth_from: from_sid}, {:ok, _result}) do
    source = session(state, from_sid)

    update_session(
      state,
      sid,
      &%{&1 | refresh_token: source.refresh_token, credentials: source.credentials}
    )
  end

  defp copy_auth(state, _sid, _method, _meta, _result), do: state

  defp apply_effects(state, _sid, _method, _params, {:error, _error}), do: state

  # A session attached again after a reconnect keeps the state being restored into it.
  defp apply_effects(state, sid, "attach", _params, {:ok, _result})
       when is_map_key(state.sessions, sid),
       do: state

  defp apply_effects(state, sid, "attach", _params, {:ok, _result}) do
    %{
      state
      | sessions: Map.put(state.sessions, sid, %Session{}),
        session_order: state.session_order ++ [sid]
    }
  end

  defp apply_effects(state, sid, "detach", _params, {:ok, _result}),
    do: forget_session(state, sid)

  # `reset` clears the session: auth, namespace, database, and variables. The server also
  # kills the session's live queries.
  defp apply_effects(state, sid, "reset", _params, {:ok, _result}) do
    state = drop_session_lives(state, sid)
    update_session(state, sid, fn _session -> %Session{} end)
  end

  defp apply_effects(state, sid, method, params, result) do
    update_session(state, sid, &session_effects(&1, method, params, result))
  end

  defp session_effects(session, "use", [namespace, database], {:ok, _result}) do
    # The server drops the database when the namespace is cleared (sent as NULL).
    database = if is_nil(namespace), do: nil, else: use_value(database, session.database)
    namespace = use_value(namespace, session.namespace)
    %{session | namespace: namespace, database: database}
  end

  defp session_effects(session, method, [credentials | _], {:ok, token})
       when method in ["signin", "signup"] and is_binary(token) do
    %{session | token: token, refresh_token: nil, credentials: credentials}
  end

  # A record access method defined `WITH REFRESH` returns an access and refresh token pair.
  defp session_effects(session, method, [credentials | _], {:ok, %{"access" => access} = tokens})
       when method in ["signin", "signup"] and is_binary(access) do
    %{session | token: access, refresh_token: tokens["refresh"], credentials: credentials}
  end

  defp session_effects(session, "authenticate", [token | _], {:ok, _result}) do
    %{session | token: token, refresh_token: nil, credentials: nil}
  end

  defp session_effects(session, method, [name, %None{}], {:ok, _result})
       when method in ["let", "set"],
       do: %{session | vars: Map.delete(session.vars, name)}

  defp session_effects(session, method, [name, value], {:ok, _result})
       when method in ["let", "set"],
       do: %{session | vars: Map.put(session.vars, name, value)}

  defp session_effects(session, "unset", [name], {:ok, _result}),
    do: %{session | vars: Map.delete(session.vars, name)}

  defp session_effects(session, "invalidate", _params, {:ok, _result}) do
    %{session | token: nil, refresh_token: nil, credentials: nil}
  end

  defp session_effects(session, _method, _params, _result), do: session

  defp use_value(%None{}, current), do: current
  defp use_value(value, _current), do: value

  defp session(state, sid), do: Map.get(state.sessions, sid, %Session{})

  defp update_session(state, sid, fun) do
    case Map.fetch(state.sessions, sid) do
      {:ok, session} -> %{state | sessions: Map.put(state.sessions, sid, fun.(session))}
      :error -> state
    end
  end

  defp forget_session(state, nil), do: state

  defp forget_session(state, sid) do
    state = drop_session_lives(state, sid)

    %{
      state
      | sessions: Map.delete(state.sessions, sid),
        session_order: List.delete(state.session_order, sid)
    }
  end

  defp drop_session_lives(state, sid) do
    state.live_specs
    |> Enum.filter(fn {_id, {live_sid, _params}} -> live_sid == sid end)
    |> Enum.reduce(state, fn {id, _spec}, state -> drop_live(state, id) end)
  end

  # Sessions and transactions are state the server keeps for one connection, so they only
  # work over a persistent (WebSocket) connection, the same as live queries.
  defp needs_sessions?(method, meta) do
    method in ["attach", "detach", "sessions", "begin", "commit", "cancel"] or
      Map.has_key?(meta, :session) or Map.has_key?(meta, :txn)
  end

  # ── Low level helpers ────────────────────────────────────────────────────

  # Encodes and sends one request, and tracks it as pending. `from` is a caller to reply
  # to, `:sync` for `rpc_sync/3`, or `nil` when nobody waits for the reply.
  defp send_request(state, id, from, method, params, meta) do
    {params, meta} = server_params(state, method, params, meta)
    sid = Map.get(meta, :session)
    payload = encode_request(state, id, method, params, sid, Map.get(meta, :txn))

    case state.engine.request(state.engine_state, id, payload, headers_for(state, sid)) do
      {:ok, engine_state} ->
        pending = Map.put(state.pending, id, {from, method, params, meta})
        {:ok, %{state | engine_state: engine_state, pending: pending}}

      # A dead WebSocket also reports the close as a transport message, which the
      # connection handles, so failing just this request is enough here.
      {:error, reason} ->
        cancel_timer(meta)
        {:error, conn_error(reason), state}
    end
  end

  defp reply(state, nil, _id, _result), do: state
  defp reply(state, :sync, id, result), do: %{state | sync: Map.put(state.sync, id, result)}

  defp reply(state, from, _id, result) do
    GenServer.reply(from, result)
    state
  end

  defp fail_pending(state, reason) do
    error = conn_error(reason)

    Enum.reduce(state.pending, %{state | pending: %{}}, fn {id, {from, _method, _params, meta}},
                                                           state ->
      cancel_timer(meta)
      reply(state, from, id, {:error, error})
    end)
  end

  defp timeout_error(method), do: {:error, Error.new(:timeout, "RPC #{method} timed out")}

  defp start_timer(meta, _id, :infinity), do: meta

  defp start_timer(meta, id, timeout) when is_integer(timeout) do
    remaining = max(timeout - waited(meta), 0)
    Map.put(meta, :timer, Process.send_after(self(), {:request_timeout, id}, remaining))
  end

  defp waited(%{sent_at: sent_at}), do: System.monotonic_time(:millisecond) - sent_at
  defp waited(_meta), do: 0

  defp out_of_time?(_meta, :infinity), do: false
  defp out_of_time?(meta, timeout), do: waited(meta) >= timeout

  # Monotonic time is only comparable within one node.
  defp local_sent_at(meta, {pid, _tag}) when node(pid) == node(), do: meta
  defp local_sent_at(meta, _from), do: Map.delete(meta, :sent_at)

  defp cancel_timer(%{timer: timer}), do: Process.cancel_timer(timer)
  defp cancel_timer(_meta), do: :ok

  defp next_id(state), do: {state.next_id, %{state | next_id: state.next_id + 1}}

  defp encode_request(state, id, method, params, sid, txn) do
    state.codec.encode(RPC.request(id, method, wire_params(state, sid, method, params), sid, txn))
  end

  # JSON has no NONE, so a "leave unchanged" `use` parameter would arrive as NULL and clear
  # the selection. Send the current selection in its place.
  defp wire_params(%{codec: SurrealDB.Codec.JSON} = state, sid, "use", [namespace, database]) do
    session = session(state, sid)
    [use_value(namespace, session.namespace), use_value(database, session.database)]
  end

  defp wire_params(_state, _sid, _method, params), do: params

  # JSON has no table type: the server reads a string such as "user:1234" as a record id,
  # so a table name with a colon would silently target a record. Refuse it rather than act
  # on the wrong thing.
  defp json_ambiguous_table(%{codec: SurrealDB.Codec.JSON}, params) do
    Enum.find_value(params, fn
      %SurrealDB.Table{name: name} when is_binary(name) ->
        if String.contains?(name, ":") do
          Error.new(
            :unsupported,
            "the table name #{inspect(name)} cannot be sent over JSON, which reads it as a " <>
              "record id; use the CBOR codec"
          )
        end

      _other ->
        nil
    end)
  end

  defp json_ambiguous_table(_state, _params), do: nil

  defp decode(state, binary), do: state.codec.decode(binary)

  defp headers_for(state, sid) do
    session = session(state, sid)
    %{namespace: session.namespace, database: session.database, token: session.token}
  end

  defp normalize_id(%SurrealDB.Uuid{value: value}), do: value
  defp normalize_id(id) when is_binary(id), do: String.downcase(id)
  defp normalize_id(id), do: to_string(id)

  defp conn_error(%Error{} = error), do: error
  defp conn_error(reason) when is_binary(reason), do: Error.new(:connection, reason)
  defp conn_error(reason), do: Error.new(:connection, inspect(reason), details: reason)

  defp resolve_engine(opts, uri) do
    case Keyword.get(opts, :engine) do
      nil -> Engine.for_scheme(uri.scheme)
      engine -> {:ok, engine}
    end
  end

  defp engine_opts(codec, opts) do
    frame_type = if codec.subprotocol() == "json", do: :text, else: :binary

    [
      content_type: codec.content_type(),
      subprotocol: codec.subprotocol(),
      frame_type: frame_type,
      codec: codec
    ]
    |> maybe_put(:transport_opts, Keyword.get(opts, :transport_opts))
    |> maybe_put(:responder, Keyword.get(opts, :responder))
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)
end
