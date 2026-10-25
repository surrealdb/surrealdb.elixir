defmodule SurrealDB.Engine do
  @moduledoc """
  Behaviour for a SurrealDB transport engine.

  An engine is a stateless module that operates on an opaque connection term owned by the
  `SurrealDB.Connection` process. Two engines ship with the SDK:

    * `SurrealDB.Engine.WebSocket` - persistent bidirectional connection, used for
      `ws://` and `wss://` URLs. Supports live queries.
    * `SurrealDB.Engine.HTTP` - request/response, used for `http://` and `https://` URLs.
      Does not support live queries.

  The connection process drives an engine like this:

    1. `connect/2` establishes the transport and returns the engine connection term.
    2. For each RPC, the connection encodes the message and calls `request/4` with the
       request id.
    3. Transport messages received by the connection process are fed to `handle_transport/2`,
       which returns a list of events. A `{:message, binary}` event carries one complete,
       still-encoded RPC response or live notification for the connection to decode.

  Correlation of responses to requests is done by the connection using the `id` inside the
  decoded message. A request-response engine (HTTP) also knows which request each reply
  belongs to, so it can fail one request by id with a `{:failed, id, error}` event when the
  reply is not an RPC message at all (for example an HTTP error status).
  """

  @typedoc "Opaque, engine-specific connection state."
  @type conn :: term()

  @typedoc """
  Per-request session context. Stateless engines (HTTP) turn this into request headers;
  stateful engines (WebSocket) ignore it because the server holds the session per socket.
  """
  @type session :: %{
          optional(:namespace) => String.t() | nil,
          optional(:database) => String.t() | nil,
          optional(:token) => String.t() | nil
        }

  @typedoc """
  An event produced from a transport message.

    * `{:message, binary}` - one complete, still-encoded RPC response or notification.
    * `:closed` - the transport is gone and cannot be used again. The connection fails
      every pending request and stops.
    * `{:reply, id, binary}` - the still-encoded reply to the request with this id, from an
      engine that knows which request a reply belongs to. If it cannot be decoded, that
      request fails instead of waiting for a reply that never comes.
    * `{:failed, id, error}` - the request with this id failed with a `SurrealDB.Error`.
    * `{:reset, reason}` - the requests in flight were lost, but the engine can carry on
      (the HTTP engine reconnects on the next request). The connection fails the pending
      requests and stays up.
  """
  @type event ::
          {:message, binary()}
          | {:reply, term(), binary()}
          | {:failed, term(), SurrealDB.Error.t()}
          | :closed
          | {:reset, term()}

  @doc "Establishes the transport for the given URI. Runs inside the connection process."
  @callback connect(uri :: URI.t(), opts :: keyword()) :: {:ok, conn()} | {:error, term()}

  @doc "Sends one already-encoded RPC payload. `id` is the id inside the payload."
  @callback request(conn(), id :: term(), payload :: iodata(), session()) ::
              {:ok, conn()} | {:error, term()}

  @doc """
  Processes a transport message delivered to the connection process. Returns `:unknown` if
  the message does not belong to this engine.
  """
  @callback handle_transport(conn(), message :: term()) ::
              {:ok, [event()], conn()} | {:error, term(), conn()} | :unknown

  @doc """
  Called when request `id` timed out and its caller has been told. An engine that cannot
  leave a request behind (HTTP/1.1 answers requests in order, so a stuck one blocks those
  after it) cleans up here, and returns events for any other requests that fail as a
  result. Optional; engines without it do nothing.
  """
  @callback cancel(conn(), id :: term()) :: {:ok, [event()], conn()}

  @doc "Closes the transport."
  @callback close(conn()) :: :ok

  @doc "Whether this engine supports live queries."
  @callback supports_live?() :: boolean()

  @doc """
  The codec the server agreed to speak, once the engine knows it. The connection encodes
  and decodes with it from then on. A WebSocket learns it from the upgrade; HTTP switches
  to `cbor.2` after the first reply in it. Optional; without it the connection keeps the
  codec it was given.
  """
  @callback codec(conn()) :: SurrealDB.Codec.t() | nil

  @optional_callbacks cancel: 2, codec: 1

  @doc "Resolves the engine module to use for a URL scheme."
  @spec for_scheme(String.t()) :: {:ok, module()} | {:error, term()}
  def for_scheme(scheme) when scheme in ["ws", "wss"], do: {:ok, SurrealDB.Engine.WebSocket}
  def for_scheme(scheme) when scheme in ["http", "https"], do: {:ok, SurrealDB.Engine.HTTP}
  def for_scheme(scheme), do: {:error, "unsupported URL scheme: #{inspect(scheme)}"}
end
