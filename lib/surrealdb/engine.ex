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
    2. For each RPC, the connection encodes the message and calls `request/3`.
    3. Transport messages received by the connection process are fed to `handle_transport/2`,
       which returns a list of events. A `{:message, binary}` event carries one complete,
       still-encoded RPC response or live notification for the connection to decode.

  Correlation of responses to requests is done by the connection using the `id` inside the
  decoded message, so engines never need to track request ids themselves.
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

  @typedoc "An event produced from a transport message."
  @type event :: {:message, binary()} | :closed

  @doc "Establishes the transport for the given URI. Runs inside the connection process."
  @callback connect(uri :: URI.t(), opts :: keyword()) :: {:ok, conn()} | {:error, term()}

  @doc "Sends one already-encoded RPC payload."
  @callback request(conn(), payload :: iodata(), session()) :: {:ok, conn()} | {:error, term()}

  @doc """
  Processes a transport message delivered to the connection process. Returns `:unknown` if
  the message does not belong to this engine.
  """
  @callback handle_transport(conn(), message :: term()) ::
              {:ok, [event()], conn()} | {:error, term(), conn()} | :unknown

  @doc "Closes the transport."
  @callback close(conn()) :: :ok

  @doc "Whether this engine supports live queries."
  @callback supports_live?() :: boolean()

  @doc "Resolves the engine module to use for a URL scheme."
  @spec for_scheme(String.t()) :: {:ok, module()} | {:error, term()}
  def for_scheme(scheme) when scheme in ["ws", "wss"], do: {:ok, SurrealDB.Engine.WebSocket}
  def for_scheme(scheme) when scheme in ["http", "https"], do: {:ok, SurrealDB.Engine.HTTP}
  def for_scheme(scheme), do: {:error, "unsupported URL scheme: #{inspect(scheme)}"}
end
