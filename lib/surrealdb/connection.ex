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
    2. authentication: the access token, or if the server refuses it, a new token pair
       from the refresh token, or signing in again with the credentials the session last
       signed in with,
    3. the namespace and database,
    4. connection parameters set with `SurrealDB.let/3`,
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
    renew_access: true,
    # session id => timer that renews the session's access token before it expires
    renewals: %{},
    # streaming query ref => request id
    streams: %{},
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
  def call(handle, method, params, opts \\ []) do
    {server, session, txn} = target(handle)
    meta = Keyword.get(opts, :meta, %{})
    meta = if opts[:timeout], do: Map.put(meta, :timeout, opts[:timeout]), else: meta
    meta = if session, do: Map.put(meta, :session, session), else: meta
    meta = if txn, do: Map.put(meta, :txn, txn), else: meta

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

  @doc """
  Starts a streaming RPC (`query_stream`) whose frames are sent to `pid` as
  `{:surrealdb, :stream, ref, frame}`, and returns `{:ok, ref}`. See
  `SurrealDB.stream_query/4` for the frames.
  """
  @spec stream(handle(), String.t(), list(), pid(), keyword()) ::
          {:ok, reference()} | {:error, Error.t()}
  def stream(handle, method, params, pid, opts \\ []) do
    {server, session, txn} = target(handle)
    meta = %{timeout: Keyword.get(opts, :timeout, :infinity)}
    meta = if session, do: Map.put(meta, :session, session), else: meta
    meta = if txn, do: Map.put(meta, :txn, txn), else: meta
    GenServer.call(server, {:stream, method, params, pid, meta}, :infinity)
  end

  @doc """
  Cancels a streaming query started with `stream/5`. No more frames are sent for it, and
  the server stops running it.
  """
  @spec cancel_stream(handle(), reference()) :: {:ok, term()} | {:error, Error.t()}
  def cancel_stream(handle, ref),
    do: GenServer.call(server(handle), {:cancel_stream, ref}, :infinity)

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

  @doc false
  # Copies the credentials a forked session needs to sign in again on its own after a
  # reconnect.
  def copy_auth(from, to) do
    {server, from_session, _txn} = target(from)
    {_server, to_session, _txn} = target(to)
    GenServer.call(server, {:copy_auth, from_session, to_session})
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
        reconnect: reconnect_config(engine, Keyword.get(opts, :reconnect, true)),
        renew_access: Keyword.get(opts, :renew_access, true)
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
  def handle_call({:rpc, method, params, meta}, from, %{status: :reconnecting} = state) do
    {id, state} = next_id(state)
    meta = start_timer(meta, id, Map.get(meta, :timeout, state.timeout))
    {:noreply, %{state | queue: [{id, from, method, params, meta} | state.queue]}}
  end

  def handle_call({:rpc, method, params, meta}, from, state) do
    if needs_sessions?(method, meta) and not state.engine.supports_live?() do
      message = "sessions and transactions need a WebSocket connection"
      {:reply, {:error, Error.new(:unsupported, message)}, state}
    else
      {id, state} = next_id(state)
      meta = start_timer(meta, id, Map.get(meta, :timeout, state.timeout))

      case send_request(state, id, from, method, params, meta) do
        {:ok, state} -> {:noreply, state}
        {:error, error, state} -> {:reply, {:error, error}, state}
      end
    end
  end

  def handle_call({:stream, method, params, pid, meta}, from, state) do
    ref = make_ref()
    {id, state} = next_id(state)
    meta = start_timer(meta, id, meta.timeout)
    state = %{state | streams: Map.put(state.streams, ref, id)}
    stream_from = {:stream, pid, ref}

    cond do
      not state.engine.supports_live?() ->
        cancel_timer(meta)
        message = "streaming queries need a WebSocket connection"
        state = %{state | streams: Map.delete(state.streams, ref)}
        {:reply, {:error, Error.new(:unsupported, message)}, state}

      state.status == :reconnecting ->
        GenServer.reply(from, {:ok, ref})
        {:noreply, %{state | queue: [{id, stream_from, method, params, meta} | state.queue]}}

      true ->
        case send_request(state, id, stream_from, method, params, meta) do
          {:ok, state} ->
            {:reply, {:ok, ref}, state}

          {:error, error, state} ->
            {:reply, {:error, error}, %{state | streams: Map.delete(state.streams, ref)}}
        end
    end
  end

  # Stop delivering the stream's frames, then ask the server to stop running it.
  def handle_call({:cancel_stream, ref}, from, state) do
    case Map.pop(state.streams, ref) do
      {nil, _streams} ->
        {:reply, {:ok, nil}, state}

      {id, streams} ->
        state = %{state | streams: streams, pending: mute_stream(state.pending, id)}
        queue = Enum.reject(state.queue, &match?({^id, _, _, _, _}, &1))
        handle_call({:rpc, "query_cancel", [id], %{}}, from, %{state | queue: queue})
    end
  end

  def handle_call({:copy_auth, from_sid, to_sid}, _from, state) do
    source = Map.get(state.sessions, from_sid, %Session{})

    state =
      update_session(state, to_sid, fn session ->
        %{session | refresh_token: source.refresh_token, credentials: source.credentials}
      end)

    {:reply, :ok, state}
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
      namespace: session.namespace,
      database: session.database,
      token: session.token,
      refresh_token: session.refresh_token,
      vars: session.vars,
      authenticated: not is_nil(session.token),
      version: state.version,
      status: state.status,
      uri: state.uri,
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
      {{from, method, _params, _meta}, pending} ->
        state = reply(%{state | pending: pending}, from, id, timeout_error(method))
        {:noreply, state}

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

  def handle_info({:renew_access, sid, token}, state) do
    state = %{state | renewals: Map.delete(state.renewals, sid)}

    # Skip a renewal the token has since been replaced for. While reconnecting, restoring
    # the session authenticates again and schedules the next renewal.
    if state.status == :connected and session(state, sid).token == token do
      {:noreply, renew(state, sid)}
    else
      {:noreply, state}
    end
  end

  def handle_info(message, %{engine_state: nil} = state) do
    Logger.debug("SurrealDB ignored a message while disconnected: #{inspect(message)}")
    {:noreply, state}
  end

  def handle_info(message, state), do: handle_transport(message, state)

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
    base = config[:delay] * :math.pow(config[:multiplier], attempt - 1)
    spread = 1 + config[:jitter] * (2 * :rand.uniform() - 1)
    round(min(base * spread, config[:max_delay]))
  end

  defp flush_queue(state) do
    state.queue
    |> Enum.reverse()
    |> Enum.reduce(%{state | queue: []}, fn {id, from, method, params, meta}, state ->
      case send_request(state, id, from, method, params, meta) do
        {:ok, state} ->
          state

        {:error, error, state} ->
          cancel_timer(meta)
          reply(state, from, id, {:error, error})
      end
    end)
  end

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

      {:ok, _other, state} ->
        {:ok, state}

      {:error, %Error{kind: :connection} = error, state} ->
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
          {:ok, _result, state} -> restore_session(state, sid)
          {:error, error, state} -> {:error, error, state}
        end

      case result do
        {:ok, state} ->
          {:cont, {:ok, state}}

        {:error, %Error{kind: :connection} = error, state} ->
          {:halt, {:error, error, state}}

        {:error, error, state} ->
          Logger.warning("SurrealDB could not restore session #{sid}: #{error.message}")
          {:cont, {:ok, forget_session(state, sid)}}
      end
    end)
  end

  # Use the token if the server still accepts it, otherwise sign in again with the stored
  # credentials. If neither works the session carries on signed out.
  # Use the token if the server still accepts it, otherwise sign in again with the stored
  # credentials. If neither works the session carries on signed out.
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
        state =
          update_session(state, sid, &%{&1 | refresh_token: refresh, credentials: credentials})

        {:ok, schedule_renewal(state, sid, "authenticate")}

      {:error, %Error{kind: :connection} = error, state} ->
        {:error, error, state}

      {:error, _unusable, state} when is_binary(refresh) ->
        restore_refresh(state, sid, token, refresh, credentials)

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

  # A refused access token can still be exchanged for a new pair with the refresh token.
  defp restore_refresh(state, sid, token, refresh, credentials) do
    case rpc_sync(state, "refresh", [%{"access" => token, "refresh" => refresh}], sid) do
      {:ok, _tokens, state} ->
        {:ok, update_session(state, sid, &%{&1 | credentials: credentials})}

      {:error, %Error{kind: :connection} = error, state} ->
        {:error, error, state}

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

  defp restore_signin(state, sid) do
    case session(state, sid).credentials do
      nil ->
        Logger.warning("SurrealDB reconnected signed out: the token is no longer valid")
        {:ok, state}

      credentials ->
        case rpc_sync(state, "signin", [credentials], sid) do
          {:ok, _result, state} ->
            {:ok, state}

          {:error, %Error{kind: :connection} = error, state} ->
            {:error, error, state}

          {:error, error, state} ->
            Logger.warning("SurrealDB reconnected signed out: #{Exception.message(error)}")
            {:ok, update_session(state, sid, &%{&1 | credentials: nil})}
        end
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
        {:error, %Error{kind: :connection} = error, state} -> {:halt, {:error, error, state}}
        {:error, _error, state} -> {:cont, {:ok, state}}
      end
    end)
  end

  # Starts every managed live query again and points its caller-facing id at the new
  # server id. A live query the server refuses to restart is ended with `:killed`.
  defp restore_live(state) do
    state = %{state | live_ids: %{}}

    Enum.reduce_while(state.live_specs, {:ok, state}, fn {id, {sid, params}}, {:ok, state} ->
      case rpc_sync(state, "live", params, sid) do
        {:ok, server_id, state} ->
          server_id = normalize_id(server_id)

          state = %{
            state
            | live_current: Map.put(state.live_current, id, server_id),
              live_ids: Map.put(state.live_ids, server_id, id)
          }

          {:cont, {:ok, state}}

        {:error, %Error{kind: :connection} = error, state} ->
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
  defp rpc_sync(state, method, params, sid \\ nil) do
    {id, state} = next_id(state)

    case send_request(state, id, :sync, method, params, %{session: sid}) do
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
        await_sync(state, id, method, deadline, [message | stash])

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
            finish_sync({:error, error, fail_pending(state, error)}, stash)
        end

      {:error, reason, engine_state} ->
        error = conn_error(reason)

        finish_sync(
          {:error, error, fail_pending(%{state | engine_state: engine_state}, error)},
          stash
        )
    end
  end

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
        {:cont, {:ok, route(Map.put_new(decoded, "id", id), state)}}

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
    case Map.get(state.pending, id) do
      {{:stream, _pid, _ref}, _method, _params, _meta} -> deliver_frame(id, result, state)
      _other -> deliver_reply(id, result, state)
    end
  end

  # A streaming request is answered by a series of frames with its id. It stays pending
  # until the `end` frame, or an error, finishes it.
  defp deliver_frame(id, result, state) do
    {{:stream, pid, ref} = from, _method, _params, meta} = Map.fetch!(state.pending, id)

    case result do
      {:ok, %{"stream" => "end"} = frame} ->
        cancel_timer(meta)
        state = %{state | pending: Map.delete(state.pending, id)}
        send_frame(pid, ref, RPC.stream_frame(frame))
        %{state | streams: Map.delete(state.streams, ref)}

      {:ok, %{"stream" => _kind} = frame} ->
        case RPC.stream_frame(frame) do
          nil -> :ok
          frame -> send_frame(pid, ref, frame)
        end

        state

      {:ok, other} ->
        cancel_timer(meta)
        state = %{state | pending: Map.delete(state.pending, id)}

        reply(
          state,
          from,
          id,
          {:error, Error.new(:decode, "unexpected stream frame: #{inspect(other)}")}
        )

      {:error, _error} = error ->
        cancel_timer(meta)
        reply(%{state | pending: Map.delete(state.pending, id)}, from, id, error)
    end
  end

  defp send_frame(nil, _ref, _frame), do: :ok
  defp send_frame(pid, ref, frame), do: send(pid, {:surrealdb, :stream, ref, frame})

  defp mute_stream(pending, id) do
    case Map.fetch(pending, id) do
      {:ok, {{:stream, _pid, ref}, method, params, meta}} ->
        Map.put(pending, id, {{:stream, nil, ref}, method, params, meta})

      _ ->
        pending
    end
  end

  defp deliver_reply(id, result, state) do
    case Map.pop(state.pending, id) do
      {nil, _pending} ->
        state

      {{from, method, params, meta}, pending} ->
        cancel_timer(meta)
        state = %{state | pending: pending}
        sid = Map.get(meta, :session)
        state = apply_effects(state, sid, method, params, result)
        state = keep_refresh_token(state, sid, method, meta, result)
        state = after_renewal(state, sid, method, meta, result)
        state = schedule_renewal(state, sid, method)
        state = register_live(state, method, params, meta, result)
        reply(state, from, id, public_reply(method, meta, result))
    end
  end

  # A reply carries the id of a pending request. Anything else whose result looks like a
  # notification is one: SurrealDB 2.x put the notification fields at the top level, 3.x
  # sends them under `result` with no top-level id. Checking for a pending id first means a
  # reply whose result happens to have an `action` field is still delivered as a reply.
  defp notification(decoded, state) do
    cond do
      Map.has_key?(state.pending, decoded["id"]) ->
        :reply

      Map.has_key?(decoded, "action") ->
        {:notification, decoded["id"], decoded}

      is_map(decoded["result"]) and Map.has_key?(decoded["result"], "action") ->
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
          state |> drop_live(id) |> kill_orphan(server_id)
        else
          %{state | live: Map.put(state.live, id, remaining)}
        end
      else
        state
      end
    end)
  end

  defp kill_orphan(%{status: :reconnecting} = state, _server_id), do: state

  defp kill_orphan(state, server_id) do
    {id, state} = next_id(state)

    case send_request(state, id, nil, "kill", [SurrealDB.Uuid.new(server_id)], %{}) do
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

  defp public_reply(method, %{statements: true}, {:ok, statements})
       when method in ["query", "gql"] and is_list(statements),
       do: {:ok, RPC.statement_results(statements)}

  defp public_reply(method, _meta, {:ok, statements})
       when method in ["query", "gql"] and is_list(statements),
       do: RPC.parse_query(statements)

  defp public_reply(_method, _meta, {:ok, value}), do: {:ok, value}

  # ── State mutation from method side effects ─────────────────────────────────

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
    namespace = use_value(namespace, session.namespace)
    # The server drops the database when the namespace is cleared.
    database = if is_nil(namespace), do: nil, else: use_value(database, session.database)
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

  defp session_effects(session, "refresh", _params, {:ok, %{"access" => access} = tokens})
       when is_binary(access) do
    %{session | token: access, refresh_token: tokens["refresh"]}
  end

  defp session_effects(session, "refresh", _params, {:ok, access}) when is_binary(access),
    do: %{session | token: access}

  defp session_effects(session, "revoke", [%{"refresh" => refresh} | _], {:ok, _result})
       when refresh == session.refresh_token,
       do: %{session | refresh_token: nil}

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

  # ── Access token renewal ──────────────────────────────────────────────────────

  # `authenticate` takes only the access token; a refresh token given alongside it rides in
  # the request meta.
  defp keep_refresh_token(state, sid, "authenticate", %{refresh_token: refresh}, {:ok, _}),
    do: update_session(state, sid, &%{&1 | refresh_token: refresh})

  defp keep_refresh_token(state, _sid, _method, _meta, _result), do: state

  # A failed refresh falls back to signing in again with the stored credentials.
  defp after_renewal(state, sid, "refresh", %{renewal: true}, {:error, error}) do
    Logger.warning("SurrealDB could not refresh the access token: #{Exception.message(error)}")
    state = update_session(state, sid, &%{&1 | refresh_token: nil})
    if session(state, sid).credentials, do: renew(state, sid), else: state
  end

  defp after_renewal(state, _sid, "signin", %{renewal: true}, {:error, error}) do
    Logger.warning("SurrealDB could not renew the access token: #{Exception.message(error)}")
    state
  end

  defp after_renewal(state, _sid, _method, _meta, _result), do: state

  @auth_methods ~w(signin signup authenticate refresh invalidate reset revoke detach)

  defp schedule_renewal(state, sid, method) when method in @auth_methods do
    {timer, renewals} = Map.pop(state.renewals, sid)
    if timer, do: Process.cancel_timer(timer)
    state = %{state | renewals: renewals}
    session = Map.get(state.sessions, sid)

    with true <- state.renew_access,
         %Session{token: token} when is_binary(token) <- session,
         true <- not is_nil(session.refresh_token) or not is_nil(session.credentials),
         {:ok, delay} <- renewal_delay(token) do
      timer = Process.send_after(self(), {:renew_access, sid, token}, delay)
      %{state | renewals: Map.put(renewals, sid, timer)}
    else
      _ -> state
    end
  end

  defp schedule_renewal(state, _sid, _method), do: state

  # Renew a minute before the token expires, or halfway through a shorter lifetime.
  @doc false
  def renewal_delay(token) do
    with {:ok, claims} <- jwt_claims(token),
         exp when is_integer(exp) <- claims["exp"] do
      now = System.system_time(:millisecond)
      lifetime = if is_integer(claims["iat"]), do: (exp - claims["iat"]) * 1000, else: nil
      margin = if lifetime, do: min(60_000, div(lifetime, 2)), else: 60_000
      {:ok, max(exp * 1000 - margin - now, 0)}
    else
      _ -> :error
    end
  end

  defp jwt_claims(token) do
    with [_header, payload, _signature] <- String.split(token, "."),
         {:ok, json} <- Base.url_decode64(payload, padding: false),
         {:ok, %{} = claims} <- Jason.decode(json) do
      {:ok, claims}
    else
      _ -> :error
    end
  end

  # Renew with the refresh token when there is one, otherwise sign in again.
  defp renew(state, sid) do
    session = session(state, sid)

    {method, params} =
      if session.refresh_token,
        do: {"refresh", [%{"access" => session.token, "refresh" => session.refresh_token}]},
        else: {"signin", [session.credentials]}

    {id, state} = next_id(state)
    meta = start_timer(%{session: sid, renewal: true}, id, state.timeout)

    case send_request(state, id, nil, method, params, meta) do
      {:ok, state} -> state
      {:error, _error, state} -> state
    end
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
    payload = state.codec.encode(RPC.request(id, method, params, sid, Map.get(meta, :txn)))

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

  # A streaming request that fails ends with an `{:error, error}` frame.
  defp reply(state, {:stream, pid, ref}, _id, result) do
    case result do
      {:error, _error} = error -> send_frame(pid, ref, error)
      _other -> :ok
    end

    %{state | streams: Map.delete(state.streams, ref)}
  end

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
    Map.put(meta, :timer, Process.send_after(self(), {:request_timeout, id}, timeout))
  end

  defp cancel_timer(%{timer: timer}), do: Process.cancel_timer(timer)
  defp cancel_timer(_meta), do: :ok

  defp next_id(state), do: {state.next_id, %{state | next_id: state.next_id + 1}}

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
