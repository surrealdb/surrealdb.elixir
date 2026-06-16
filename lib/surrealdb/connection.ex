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
    next_id: 1,
    pending: %{},
    live: %{}
  ]

  @default_connect_timeout 15_000

  @doc "Starts a connection process. See `SurrealDB.start_link/1` for the options."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @doc "Performs an RPC call and returns the transformed reply."
  @spec call(GenServer.server(), String.t(), list(), keyword()) ::
          {:ok, term()} | {:error, Error.t()}
  def call(server, method, params, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 30_000)
    meta = Keyword.get(opts, :meta, %{})

    try do
      GenServer.call(server, {:rpc, method, params, meta}, timeout)
    catch
      :exit, {:timeout, _} -> {:error, Error.new(:timeout, "RPC #{method} timed out")}
    end
  end

  @doc "Registers `pid` to receive notifications for an existing live query id."
  @spec subscribe_live(GenServer.server(), term(), pid()) :: :ok
  def subscribe_live(server, live_id, pid) do
    GenServer.call(server, {:subscribe_live, live_id, pid})
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
      state = %__MODULE__{engine: engine, engine_state: engine_state, codec: codec, uri: uri}
      setup(state, opts)
    else
      {:error, reason} ->
        {:stop, Error.new(:connection, "could not connect: #{inspect(reason)}", details: reason)}
    end
  end

  # The connect-time `use` and `signin` run synchronously here, inside init, where the
  # caller is still blocked and holds no pid. That keeps the synchronous receive loop from
  # ever stealing a `$gen_call` issued by a user once start_link returns.
  defp setup(state, opts) do
    namespace = Keyword.get(opts, :namespace)
    database = Keyword.get(opts, :database)
    auth = Keyword.get(opts, :auth)
    timeout = Keyword.get(opts, :connect_timeout, @default_connect_timeout)

    state = %{state | namespace: namespace, database: database}

    with {:ok, state} <- maybe_use(state, namespace, database, timeout),
         {:ok, state} <- maybe_signin(state, auth, timeout) do
      {:ok, state}
    else
      {:error, reason, _state} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:rpc, method, params, meta}, from, state) do
    {id, state} = next_id(state)
    payload = encode_request(state, id, method, params)

    case state.engine.request(state.engine_state, payload, session(state)) do
      {:ok, engine_state} ->
        pending = Map.put(state.pending, id, {from, method, params, meta})
        {:noreply, %{state | engine_state: engine_state, pending: pending}}

      {:error, reason} ->
        {:reply, {:error, conn_error(reason)}, state}
    end
  end

  def handle_call({:subscribe_live, live_id, pid}, _from, state) do
    {:reply, :ok, %{state | live: Map.put(state.live, normalize_id(live_id), pid)}}
  end

  def handle_call(:session_info, _from, state) do
    info = %{
      namespace: state.namespace,
      database: state.database,
      token: state.token,
      authenticated: not is_nil(state.token),
      uri: state.uri,
      engine: state.engine,
      codec: state.codec,
      supports_live?: state.engine.supports_live?()
    }

    {:reply, info, state}
  end

  @impl true
  def handle_info(message, state) do
    case state.engine.handle_transport(state.engine_state, message) do
      :unknown ->
        {:noreply, state}

      {:ok, events, engine_state} ->
        state = Enum.reduce(events, %{state | engine_state: engine_state}, &process_event/2)
        {:noreply, state}

      {:error, reason, engine_state} ->
        Logger.warning("SurrealDB transport error: #{inspect(reason)}")
        {:noreply, fail_pending(%{state | engine_state: engine_state}, reason)}
    end
  end

  @impl true
  def terminate(_reason, state) do
    if state.engine_state, do: state.engine.close(state.engine_state)
    :ok
  end

  # ── Setup helpers ────────────────────────────────────────────────────────

  defp maybe_use(state, nil, nil, _timeout), do: {:ok, state}

  defp maybe_use(state, namespace, database, timeout) do
    case rpc_sync(state, "use", [namespace, database], timeout) do
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

    case state.engine.request(state.engine_state, payload, session(state)) do
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

  # ── Event processing (async path) ─────────────────────────────────────────

  defp process_event(:closed, state), do: fail_pending(state, "connection closed")

  defp process_event({:message, binary}, state) do
    case decode(state, binary) do
      {:ok, decoded} ->
        route(decoded, state)

      {:error, reason} ->
        Logger.warning("SurrealDB could not decode message: #{inspect(reason)}")
        state
    end
  end

  defp route(decoded, state) do
    case notification(decoded) do
      {:notification, live_id, action, result} ->
        dispatch_live(state, live_id, action, result)
        state

      :reply ->
        deliver_reply(decoded, state)
    end
  end

  defp deliver_reply(decoded, state) do
    id = decoded["id"]

    case Map.pop(state.pending, id) do
      {nil, _pending} ->
        state

      {{from, method, params, meta}, pending} ->
        result = to_result(decoded)
        state = %{state | pending: pending}
        state = apply_side_effects(state, method, params, result)
        state = register_live(state, method, meta, result)
        GenServer.reply(from, public_reply(method, meta, result))
        state
    end
  end

  defp notification(decoded) do
    cond do
      Map.has_key?(decoded, "action") ->
        {:notification, decoded["id"], decoded["action"], decoded["result"]}

      is_map(decoded["result"]) and Map.has_key?(decoded["result"], "action") ->
        inner = decoded["result"]
        {:notification, inner["id"], inner["action"], inner["result"]}

      true ->
        :reply
    end
  end

  defp dispatch_live(state, live_id, action, result) do
    case Map.get(state.live, normalize_id(live_id)) do
      nil ->
        :ok

      pid ->
        send(pid, {:surrealdb, :live, live_id, %{action: RPC.action(action), result: result}})
    end
  end

  defp register_live(state, "live", %{subscriber: pid}, {:ok, live_id}) when is_pid(pid) do
    %{state | live: Map.put(state.live, normalize_id(live_id), pid)}
  end

  defp register_live(state, "kill", %{live_id: live_id}, {:ok, _result}) do
    %{state | live: Map.delete(state.live, normalize_id(live_id))}
  end

  defp register_live(state, _method, _meta, _result), do: state

  # ── Result shaping ─────────────────────────────────────────────────────────

  defp to_result(%{"error" => error}), do: {:error, Error.from_rpc(error)}
  defp to_result(%{"result" => result}), do: {:ok, result}
  defp to_result(_decoded), do: {:ok, nil}

  defp public_reply(_method, _meta, {:error, _error} = error), do: error

  defp public_reply("query", %{raw: true}, {:ok, statements}), do: {:ok, statements}

  defp public_reply("query", _meta, {:ok, statements}) when is_list(statements),
    do: RPC.parse_query(statements)

  defp public_reply(_method, _meta, {:ok, value}), do: {:ok, value}

  # ── State mutation from method side effects ─────────────────────────────────

  defp apply_side_effects(state, _method, _params, {:error, _error}), do: state

  defp apply_side_effects(state, "use", params, {:ok, _result}) do
    namespace = Enum.at(params, 0)
    database = Enum.at(params, 1)

    %{
      state
      | namespace: namespace || state.namespace,
        database: database || state.database
    }
  end

  defp apply_side_effects(state, method, _params, {:ok, token})
       when method in ["signin", "signup"] and is_binary(token) do
    %{state | token: token}
  end

  defp apply_side_effects(state, "authenticate", params, {:ok, _result}) do
    %{state | token: Enum.at(params, 0)}
  end

  defp apply_side_effects(state, "invalidate", _params, {:ok, _result}) do
    %{state | token: nil}
  end

  defp apply_side_effects(state, _method, _params, _result), do: state

  # ── Low level helpers ────────────────────────────────────────────────────

  defp fail_pending(state, reason) do
    error = conn_error(reason)

    Enum.each(state.pending, fn {_id, {from, _method, _params, _meta}} ->
      GenServer.reply(from, {:error, error})
    end)

    %{state | pending: %{}}
  end

  defp next_id(state), do: {state.next_id, %{state | next_id: state.next_id + 1}}

  defp encode_request(state, id, method, params) do
    state.codec.encode(RPC.request(id, method, params))
  end

  defp decode(state, binary), do: state.codec.decode(binary)

  defp session(state) do
    %{namespace: state.namespace, database: state.database, token: state.token}
  end

  defp normalize_id(%SurrealDB.Uuid{value: value}), do: value
  defp normalize_id(id) when is_binary(id), do: id
  defp normalize_id(id), do: to_string(id)

  defp conn_error(%Error{} = error), do: error
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
