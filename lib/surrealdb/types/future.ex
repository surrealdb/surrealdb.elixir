defmodule SurrealDB.Future do
  @moduledoc """
  Marker for a SurrealDB computed future value (CBOR tag 15).

  The server sends a future as an unevaluated marker. The `inner` field holds whatever the
  protocol carried, which is usually `nil`.
  """

  defstruct inner: nil

  @type t :: %__MODULE__{inner: term()}

  @doc "Builds a future marker."
  @spec new(term()) :: t()
  def new(inner \\ nil), do: %__MODULE__{inner: inner}
end
