defmodule SurrealDB.Bytes do
  @moduledoc """
  A SurrealDB bytes value.

  Wrapping a binary in `Bytes` distinguishes it from a UTF-8 string so the CBOR protocol
  encodes it as a byte string (major type 2) rather than a text string.

      iex> SurrealDB.Bytes.new(<<1, 2, 3>>)
      %SurrealDB.Bytes{data: <<1, 2, 3>>}
  """

  @enforce_keys [:data]
  defstruct [:data]

  @type t :: %__MODULE__{data: binary()}

  @doc "Wraps a binary as a bytes value."
  @spec new(binary()) :: t()
  def new(data) when is_binary(data), do: %__MODULE__{data: data}

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(%SurrealDB.Bytes{data: data}, _opts) do
      concat(["#SurrealDB.Bytes<", Integer.to_string(byte_size(data)), " bytes>"])
    end
  end
end
