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
  """

  use GenServer

  require Logger

  alias SurrealDB.{Codec, Engine, Error, RPC}

  defstruct [
    :engine,
    :engine_state,
    :codec,
    :uri,
    :namespace,
    :database,
    :token,
    :refresh_token,
    :version,
    :timeout,
    # transport options from start_link, for the one-off HTTP requests (health, export)
    :transport_opts,
    next_id: 1,
    pending: %{},
    # live query id => MapSet of subscriber pids
    live: %{},
    # subscriber pid => monitor ref
    monitors: %{}
  ]

  @default_connect_timeout 15_000
  @default_timeout 30_000

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
  @spec call(GenServer.server(), String.t(), list(), keyword()) ::
          {:ok, term()} | {:error, Error.t()}
  def call(server, method, params, opts \\ []) do
    meta = Keyword.get(opts, :meta, %{})
    meta = if opts[:timeout], do: Map.put(meta, :timeout, opts[:timeout]), else: meta

    # The connection enforces the timeout and always replies, so this only waits forever
    # if the connection process itself is stuck, and the call exits if it dies.
    GenServer.call(server, {:rpc, method, params, meta}, :infinity)
  end

  @doc "Adds `pid` to the subscribers of a live query id."
  @spec subscribe_live(GenServer.server(), term(), pid()) :: :ok
  def subscribe_live(server, live_id, pid) do
    GenServer.call(server, {:subscribe_live, live_id, pid})
  end

  @doc "Removes `pid` from the subscribers of a live query id."
  @spec unsubscribe_live(GenServer.server(), term(), pid()) :: :ok
  def unsubscribe_live(server, live_id, pid) do
    GenServer.call(server, {:unsubscribe_live, live_id, pid})
  end

  @doc "Returns the active session info: namespace, database, whether a token is set, engine."
  @spec session_info(GenServer.server()) :: map()
  def session_info(server), do: GenServer.call(server, :session_info)

  # ── GenServer callbacks ─────────────────────────────────────────────────

  @impl true
  def init(opts) do
    url = Keyword.fetch!(opts, :url)
    uri = URI.parse(url)
    codec = Codec.resolve(Keyword.get(opts, :codec, :cbor))

    with {:ok, engine} <- resolve_engine(opts, uri),
         {:ok, engine_state} <- engine.connect(uri, engine_opts(codec, opts)) do
      state = %__MODULE__{
        engine: engine,
        engine_state: engine_state,
        codec: codec,
        uri: uri,
        timeout: Keyword.get(opts, :timeout, @default_timeout),
        transport_opts: Keyword.get(opts, :transport_opts)
      }

      setup(state, opts)
    else
      {:error, reason} ->
        {:stop, Error.new(:connection, "could not connect: #{inspect(reason)}", details: reason)}
    end
  end

  # Sign in before selecting the namespace and database: the server only creates a missing
  # namespace or database for a caller allowed to, so a `use` sent before `signin` leaves a
  # fresh database undefined and every later query fails.
  #
  # The connect-time `signin` and `use` run synchronously here, inside init, where the
  # caller is still blocked and holds no pid. That keeps the synchronous receive loop from
  # ever stealing a `$gen_call` issued by a user once start_link returns.
  defp setup(state, opts) do
    namespace = Keyword.get(opts, :namespace)
    database = Keyword.get(opts, :database)
    auth = Keyword.get(opts, :auth)
    timeout = Keyword.get(opts, :connect_timeout, @default_connect_timeout)

    state = %{state | namespace: namespace, database: database}

    with {:ok, state} <- check_version(state, Keyword.get(opts, :version_check, true), timeout),
         {:ok, state} <- maybe_signin(state, auth, timeout),
         {:ok, state} <- maybe_use(state, namespace, database, timeout) do
      {:ok, state}
    else
      {:error, reason, _state} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:rpc, method, params, meta}, from, state) do
    case json_ambiguous_table(state, params) do
      nil -> send_rpc(method, params, meta, from, state)
      error -> {:reply, {:error, error}, state}
    end
  end

  def handle_call({:subscribe_live, live_id, pid}, _from, state) do
    {:reply, :ok, add_subscriber(state, normalize_id(live_id), pid)}
  end

  def handle_call({:unsubscribe_live, live_id, pid}, _from, state) do
    {:reply, :ok, remove_subscriber(state, normalize_id(live_id), pid)}
  end

  def handle_call(:session_info, _from, state) do
    info = %{
      namespace: state.namespace,
      database: state.database,
      token: state.token,
      authenticated: not is_nil(state.token),
      version: state.version,
      uri: state.uri,
      transport_opts: state.transport_opts,
      engine: state.engine,
      codec: state.codec,
      supports_live?: state.engine.supports_live?()
    }

    {:reply, info, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason} = message, state) do
    if Map.get(state.monitors, pid) == ref do
      {:noreply, subscriber_down(state, pid)}
    else
      handle_transport(message, state)
    end
  end

  def handle_info({:request_timeout, id}, state) do
    case Map.pop(state.pending, id) do
      {{from, method, params, meta}, pending} ->
        if from,
          do: GenServer.reply(from, {:error, Error.new(:timeout, "RPC #{method} timed out")})

        pending = await_late_reply(state, pending, id, method, params, meta)
        cancel_in_engine(%{state | pending: pending}, id)

      {nil, _pending} ->
        {:noreply, state}
    end
  end

  def handle_info(message, state), do: handle_transport(message, state)

  # Methods whose reply changes what the connection tracks about the session.
  @session_methods ~w(use signin signup authenticate invalidate reset let set unset)

  # A timed-out request on an engine that cannot cancel it (a WebSocket) may still run on
  # the server. If it changes session state, keep it pending with no caller, so its late
  # reply still updates the namespace, database, and auth the connection reports and sends.
  # A late `live` reply starts a query nobody listens to, so it is killed. Any other request
  # keeps a tombstone, so its late reply is dropped instead of being read as a notification.
  defp await_late_reply(state, pending, id, method, params, meta) do
    cond do
      function_exported?(state.engine, :cancel, 2) ->
        pending

      method == "live" ->
        Map.put(pending, id, {nil, method, params, %{late_live: true}})

      method in @session_methods ->
        Map.put(pending, id, {nil, method, params, Map.delete(meta, :timer)})

      true ->
        Map.put(pending, id, {nil, method, params, %{discard: true}})
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

  # The transport is gone for good. Fail every caller and stop, so a supervisor can start a
  # fresh connection instead of this process lingering with a dead socket.
  defp connection_lost(state, reason) do
    error = conn_error(reason)
    Logger.warning("SurrealDB connection lost: #{Exception.message(error)}")
    {:stop, {:shutdown, {:connection_lost, error}}, fail_pending(state, error)}
  end

  @impl true
  def terminate(_reason, state) do
    if state.engine_state, do: state.engine.close(state.engine_state)
    :ok
  end

  # ── Setup helpers ────────────────────────────────────────────────────────

  # Ask the server for its version first, and refuse servers outside the supported range.
  defp check_version(state, check?, timeout) do
    case rpc_sync(state, "version", [], timeout) do
      {:ok, version, state} when is_binary(version) ->
        state = %{state | version: version}

        case check? && SurrealDB.Version.check(version) do
          {:error, error} -> {:error, error, state}
          _ok -> {:ok, state}
        end

      {:ok, other, state} ->
        if check? do
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
        if check?, do: {:error, error, state}, else: {:ok, state}
    end
  end

  defp maybe_use(state, nil, nil, _timeout), do: {:ok, state}

  defp maybe_use(state, namespace, database, timeout) do
    params = [namespace || SurrealDB.None.none(), database || SurrealDB.None.none()]

    case rpc_sync(state, "use", params, timeout) do
      {:ok, _result, state} -> {:ok, state}
      {:error, reason, state} -> {:error, reason, state}
    end
  end

  defp maybe_signin(state, nil, _timeout), do: {:ok, state}

  defp maybe_signin(state, auth, timeout) do
    case rpc_sync(state, "signin", [SurrealDB.Auth.to_params(auth)], timeout) do
      {:ok, _result, state} -> {:ok, state}
      {:error, reason, state} -> {:error, reason, state}
    end
  end

  # Synchronous RPC used during setup, before the message loop handles replies itself.
  defp rpc_sync(state, method, params, timeout) do
    {id, state} = next_id(state)
    payload = encode_request(state, id, method, params)

    case state.engine.request(state.engine_state, id, payload, session(state)) do
      {:ok, engine_state} ->
        await_reply(%{state | engine_state: engine_state}, id, method, params, timeout)

      {:error, reason} ->
        {:error, conn_error(reason), state}
    end
  end

  defp await_reply(state, id, method, params, timeout) do
    receive do
      message ->
        case state.engine.handle_transport(state.engine_state, message) do
          :unknown ->
            await_reply(state, id, method, params, timeout)

          {:ok, events, engine_state} ->
            state = %{state | engine_state: engine_state}

            case scan_for_reply(events, state, id) do
              {:found, result, state} ->
                state = apply_side_effects(state, method, params, result)

                case result do
                  {:ok, value} -> {:ok, value, state}
                  {:error, error} -> {:error, error, state}
                end

              {:not_found, state} ->
                await_reply(state, id, method, params, timeout)
            end

          {:error, reason, engine_state} ->
            {:error, conn_error(reason), %{state | engine_state: engine_state}}
        end
    after
      timeout -> {:error, Error.new(:timeout, "RPC #{method} timed out during setup"), state}
    end
  end

  defp scan_for_reply([], state, _id), do: {:not_found, state}

  defp scan_for_reply([{:message, binary} | rest], state, id) do
    case decode(state, binary) do
      {:ok, %{"id" => ^id} = decoded} -> {:found, to_result(decoded), state}
      _ -> scan_for_reply(rest, state, id)
    end
  end

  defp scan_for_reply([:closed | _rest], state, _id) do
    {:found, {:error, Error.new(:connection, "connection closed")}, state}
  end

  defp scan_for_reply([{:reply, reply_id, binary} | rest], state, id) do
    case decode(state, binary) do
      {:ok, %{} = decoded} ->
        scan_for_reply([{:decoded, Map.put(decoded, "id", reply_id)} | rest], state, id)

      _other when reply_id == id ->
        {:found, {:error, Error.new(:decode, "could not decode the reply", details: binary)},
         state}

      _other ->
        scan_for_reply(rest, state, id)
    end
  end

  defp scan_for_reply([{:decoded, %{"id" => id} = decoded} | _rest], state, id),
    do: {:found, to_result(decoded), state}

  defp scan_for_reply([{:decoded, _decoded} | rest], state, id),
    do: scan_for_reply(rest, state, id)

  defp scan_for_reply([{:failed, id, error} | _rest], state, id) do
    {:found, {:error, error}, state}
  end

  defp scan_for_reply([{:failed, _other, _error} | rest], state, id),
    do: scan_for_reply(rest, state, id)

  defp scan_for_reply([{:reset, reason} | _rest], state, _id) do
    {:found, {:error, conn_error(reason)}, state}
  end

  # ── Event processing (async path) ─────────────────────────────────────────

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
      {:ok, decoded} ->
        {:cont, {:ok, route(decoded, state)}}

      {:error, reason} ->
        Logger.warning("SurrealDB could not decode message: #{inspect(reason)}")
        {:cont, {:ok, state}}
    end
  end

  defp route(decoded, state) do
    case notification(decoded, state) do
      {:notification, live_id, notification} ->
        dispatch_live(state, live_id, notification)

      :reply ->
        deliver_reply(decoded, state)
    end
  end

  defp deliver_reply(decoded, state), do: deliver_result(decoded["id"], to_result(decoded), state)

  defp deliver_result(id, result, state) do
    case Map.pop(state.pending, id) do
      {nil, _pending} ->
        state

      {{from, method, params, meta}, pending} ->
        cancel_timer(meta)
        state = %{state | pending: pending}
        state = apply_side_effects(state, method, params, result)
        state = register_live(state, method, meta, result)
        if from, do: GenServer.reply(from, public_reply(method, meta, result))
        state
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

  defp dispatch_live(state, live_id, notification) do
    id = normalize_id(live_id)
    action = RPC.action(notification["action"])

    message =
      {:surrealdb, :live, live_id,
       %{action: action, result: notification["result"], record: notification["record"]}}

    state.live |> Map.get(id, MapSet.new()) |> Enum.each(&send(&1, message))

    # KILLED is the last notification a live query sends: the server ended it (KILL, a
    # removed table, a revoked principal), so forget it and its subscribers.
    if action == :killed, do: drop_live(state, id), else: state
  end

  defp register_live(state, "live", %{late_live: true}, {:ok, live_id}),
    do: kill_orphan(state, normalize_id(live_id))

  defp register_live(state, "live", %{subscriber: pid}, {:ok, live_id}) when is_pid(pid) do
    add_subscriber(state, normalize_id(live_id), pid)
  end

  defp register_live(state, "kill", %{live_id: live_id}, {:ok, _result}) do
    drop_live(state, normalize_id(live_id))
  end

  defp register_live(state, _method, _meta, _result), do: state

  # ── Live subscribers ───────────────────────────────────────────────────────

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
    demonitor_unused(%{state | live: live}, MapSet.to_list(subscribers))
  end

  # A subscriber exited. Drop it everywhere, and kill every live query it was the last
  # subscriber of, so the server stops sending notifications nobody will read.
  defp subscriber_down(state, pid) do
    state = %{state | monitors: Map.delete(state.monitors, pid)}

    Enum.reduce(state.live, state, fn {id, subscribers}, state ->
      if MapSet.member?(subscribers, pid) do
        remaining = MapSet.delete(subscribers, pid)

        if MapSet.size(remaining) == 0 do
          state |> Map.update!(:live, &Map.delete(&1, id)) |> kill_orphan(id)
        else
          %{state | live: Map.put(state.live, id, remaining)}
        end
      else
        state
      end
    end)
  end

  defp kill_orphan(state, id) do
    {request_id, state} = next_id(state)
    payload = encode_request(state, request_id, "kill", [SurrealDB.Uuid.new(id)])

    case state.engine.request(state.engine_state, request_id, payload, session(state)) do
      {:ok, engine_state} ->
        pending = Map.put(state.pending, request_id, {nil, "kill", [id], %{}})
        %{state | engine_state: engine_state, pending: pending}

      {:error, _reason} ->
        state
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

  defp public_reply(_method, _meta, {:ok, value}), do: {:ok, value}

  # ── State mutation from method side effects ─────────────────────────────────

  defp apply_side_effects(state, _method, _params, {:error, _error}), do: state

  defp apply_side_effects(state, "use", [namespace, database], {:ok, _result}) do
    # The server drops the database when the namespace is cleared (sent as NULL).
    database = if is_nil(namespace), do: nil, else: use_value(database, state.database)
    %{state | namespace: use_value(namespace, state.namespace), database: database}
  end

  defp apply_side_effects(state, method, _params, {:ok, token})
       when method in ["signin", "signup"] and is_binary(token) do
    %{state | token: token, refresh_token: nil}
  end

  # A record access method defined `WITH REFRESH` returns an access and refresh token pair.
  defp apply_side_effects(state, method, _params, {:ok, %{"access" => access} = tokens})
       when method in ["signin", "signup"] and is_binary(access) do
    %{state | token: access, refresh_token: tokens["refresh"]}
  end

  defp apply_side_effects(state, "authenticate", params, {:ok, _result}) do
    %{state | token: Enum.at(params, 0), refresh_token: nil}
  end

  # `reset` clears the session: auth, namespace, database, and variables. The server also
  # kills the session's live queries.
  defp apply_side_effects(state, "reset", _params, {:ok, _result}) do
    state = Enum.reduce(Map.keys(state.live), state, &drop_live(&2, &1))
    %{state | namespace: nil, database: nil, token: nil, refresh_token: nil}
  end

  defp apply_side_effects(state, "invalidate", _params, {:ok, _result}) do
    %{state | token: nil, refresh_token: nil}
  end

  defp apply_side_effects(state, _method, _params, _result), do: state

  defp use_value(%SurrealDB.None{}, current), do: current
  defp use_value(value, _current), do: value

  # ── Low level helpers ────────────────────────────────────────────────────

  defp fail_pending(state, reason) do
    error = conn_error(reason)

    # Internal requests (an orphaned live query's `kill`) have no caller to tell.
    Enum.each(state.pending, fn {_id, {from, _method, _params, meta}} ->
      cancel_timer(meta)
      if from, do: GenServer.reply(from, {:error, error})
    end)

    %{state | pending: %{}}
  end

  defp start_timer(meta, _id, :infinity), do: meta

  defp start_timer(meta, id, timeout) when is_integer(timeout) do
    Map.put(meta, :timer, Process.send_after(self(), {:request_timeout, id}, timeout))
  end

  defp cancel_timer(%{timer: timer}), do: Process.cancel_timer(timer)
  defp cancel_timer(_meta), do: :ok

  defp next_id(state), do: {state.next_id, %{state | next_id: state.next_id + 1}}

  defp send_rpc(method, params, meta, from, state) do
    {id, state} = next_id(state)
    payload = encode_request(state, id, method, params)

    case state.engine.request(state.engine_state, id, payload, session(state)) do
      {:ok, engine_state} ->
        meta = start_timer(meta, id, Map.get(meta, :timeout, state.timeout))
        pending = Map.put(state.pending, id, {from, method, params, meta})
        {:noreply, %{state | engine_state: engine_state, pending: pending}}

      # A dead WebSocket also reports the close as a transport message, which stops the
      # connection, so failing just this request is enough here.
      {:error, reason} ->
        {:reply, {:error, conn_error(reason)}, state}
    end
  end

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

  defp encode_request(state, id, method, params) do
    state.codec.encode(RPC.request(id, method, wire_params(state, method, params)))
  end

  # JSON has no NONE, so a "leave unchanged" `use` parameter would arrive as NULL and clear
  # the selection. Send the current selection in its place.
  defp wire_params(%{codec: SurrealDB.Codec.JSON} = state, "use", [namespace, database]),
    do: [use_value(namespace, state.namespace), use_value(database, state.database)]

  defp wire_params(_state, _method, params), do: params

  defp decode(state, binary), do: state.codec.decode(binary)

  defp session(state) do
    %{namespace: state.namespace, database: state.database, token: state.token}
  end

  defp normalize_id(%SurrealDB.Uuid{value: value}), do: value
  defp normalize_id(id) when is_binary(id), do: id
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
