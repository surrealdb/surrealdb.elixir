defmodule SurrealDB.Codec.CBOR2 do
  @moduledoc """
  The `cbor.2` protocol codec, which SurrealDB 4.0 adds.

  `cbor.2` is plain `cbor` (see `SurrealDB.Codec.CBOR`) plus tags 180 to 188 for the sized
  numbers, which decode to and encode from `SurrealDB.Number`. It is the WebSocket
  subprotocol `cbor.2` and the HTTP content type `application/cbor; v=2`.

  You rarely pick this codec by hand. With the default `codec: :cbor`, a connection offers
  `cbor.2` and falls back to `cbor` when the server does not speak it, as servers before
  4.0 do. Choose `codec: :cbor2` only to require `cbor.2`.
  """

  @behaviour SurrealDB.Codec

  alias SurrealDB.Codec.CBOR

  @impl true
  def content_type, do: "application/cbor; v=2"

  @impl true
  def subprotocol, do: "cbor.2"

  @impl true
  def encode(term), do: CBOR.encode(term, :cbor2)

  @impl true
  def decode(binary), do: CBOR.decode(binary, :cbor2)
end
