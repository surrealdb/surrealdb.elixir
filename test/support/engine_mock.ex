defmodule SurrealDB.Engine.Mock do
  @moduledoc """
  In-memory engine for tests.

  It implements the `SurrealDB.Engine` behaviour without any network. A `:responder`
  function decides the result for each RPC, and the engine echoes a correctly framed
  response back to the connection process so the full request/reply path is exercised.

  Live notifications can be injected in a test by sending the connection process a
  `{:mock_message, encoded_payload}` message; `handle_transport/2` forwards it like any
  other engine message. Send `:mock_closed` to simulate a dead transport, or
  `{:mock_reset, reason}` to simulate lost in-flight requests. A responder that returns
  `:noreply` leaves the request unanswered, and `{:frames, results}` answers it with one
  response per result, as a streaming query is answered. A responder of arity 3 also gets the decoded
  request, to check its `"session"` and `"txn"` fields.
  """

  @behaviour SurrealDB.Engine

  defstruct [:codec, :responder]

  @impl true
  def connect(_uri, opts) do
    codec = SurrealDB.Codec.resolve(Keyword.get(opts, :codec, :cbor))
    responder = Keyword.get(opts, :responder, &default_responder/2)

    # A responder can refuse the connection by answering the pseudo-method "__connect__"
    # with `{:error, reason}`.
    case respond(responder, "__connect__", [], %{}) do
      {:error, reason} -> {:error, reason}
      _ -> {:ok, %__MODULE__{codec: codec, responder: responder}}
    end
  end

  @impl true
  def request(%__MODULE__{codec: codec, responder: responder} = conn, _id, payload, _session) do
    {:ok, request} = codec.decode(IO.iodata_to_binary(payload))
    id = request["id"]
    method = request["method"]
    params = request["params"] || []

    case respond(responder, method, params, request) do
      :noreply ->
        :ok

      {:failed, error} ->
        send(self(), {:mock_failed, id, error})

      {:ok, result} ->
        send(self(), {:mock_message, codec.encode(%{"id" => id, "result" => result})})

      {:frames, results} ->
        for result <- results do
          send(self(), {:mock_message, codec.encode(%{"id" => id, "result" => result})})
        end

      {:error, error} ->
        send(self(), {:mock_message, codec.encode(%{"id" => id, "error" => error})})
    end

    {:ok, conn}
  end

  # A 3-arity responder also gets the whole request, to see its session and txn.
  defp respond(responder, method, params, request) when is_function(responder, 3),
    do: responder.(method, params, request)

  defp respond(responder, method, params, _request), do: responder.(method, params)

  @impl true
  def handle_transport(conn, {:mock_message, payload}) do
    {:ok, [{:message, IO.iodata_to_binary(payload)}], conn}
  end

  def handle_transport(conn, {:mock_failed, id, error}), do: {:ok, [{:failed, id, error}], conn}
  def handle_transport(conn, :mock_closed), do: {:ok, [:closed], conn}
  def handle_transport(conn, {:mock_reset, reason}), do: {:ok, [{:reset, reason}], conn}
  def handle_transport(_conn, _message), do: :unknown

  @impl true
  def close(_conn), do: :ok

  @impl true
  def supports_live?, do: true

  defp default_responder("__connect__", _params), do: :ok
  defp default_responder("use", _params), do: {:ok, nil}
  defp default_responder("signin", _params), do: {:ok, "mock.jwt.token"}
  defp default_responder("signup", _params), do: {:ok, "mock.jwt.token"}
  defp default_responder("authenticate", _params), do: {:ok, nil}
  defp default_responder("version", _params), do: {:ok, "surrealdb-3.0.0"}
  defp default_responder(_method, params), do: {:ok, params}
end
