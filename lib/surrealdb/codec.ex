defmodule SurrealDB.Codec do
  @moduledoc """
  Behaviour for the wire protocol used to talk to SurrealDB.

  A codec turns Elixir terms (including the `SurrealDB` value types) into the bytes sent
  over an engine, and back again. Two codecs ship with the SDK:

    * `SurrealDB.Codec.CBOR` - the default, with full SurrealDB type fidelity.
    * `SurrealDB.Codec.JSON` - a JSON fallback for scalars and plain structures.

  Pick one per connection with the `:codec` option.
  """

  @typedoc "A codec module."
  @type t :: module()

  @doc "Encodes an Elixir term to its wire bytes."
  @callback encode(term()) :: iodata()

  @doc "Decodes wire bytes back to an Elixir term."
  @callback decode(binary()) :: {:ok, term()} | {:error, term()}

  @doc "The MIME content type this codec speaks (used for HTTP and WebSocket negotiation)."
  @callback content_type() :: String.t()

  @doc "The WebSocket subprotocol name for this codec."
  @callback subprotocol() :: String.t()

  @doc "Resolves a codec name or module to a codec module."
  @spec resolve(atom() | module()) :: t()
  def resolve(:cbor), do: SurrealDB.Codec.CBOR
  def resolve(:json), do: SurrealDB.Codec.JSON
  def resolve(module) when is_atom(module), do: module
end
