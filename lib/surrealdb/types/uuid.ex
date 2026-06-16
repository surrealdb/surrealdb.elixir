defmodule SurrealDB.Uuid do
  @moduledoc """
  A UUID value.

  The canonical lowercase string form is stored. Over CBOR it is encoded as a binary UUID
  (tag 37); over JSON it is a string.

      iex> SurrealDB.Uuid.new("0190d6f2-8b1e-7000-8000-000000000000")
      %SurrealDB.Uuid{value: "0190d6f2-8b1e-7000-8000-000000000000"}
  """

  @enforce_keys [:value]
  defstruct [:value]

  @type t :: %__MODULE__{value: String.t()}

  @doc "Wraps a UUID string."
  @spec new(String.t()) :: t()
  def new(value) when is_binary(value), do: %__MODULE__{value: String.downcase(value)}

  @doc "Builds a UUID from its 16 raw bytes."
  @spec from_binary(binary()) :: t()
  def from_binary(<<a::32, b::16, c::16, d::16, e::48>>) do
    value =
      [a, b, c, d, e]
      |> Enum.zip([8, 4, 4, 4, 12])
      |> Enum.map_join("-", fn {n, width} ->
        n |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(width, "0")
      end)

    %__MODULE__{value: value}
  end

  @doc "Returns the 16 raw bytes of the UUID."
  @spec to_binary(t()) :: binary()
  def to_binary(%__MODULE__{value: value}) do
    hex = value |> String.replace("-", "")
    {int, ""} = Integer.parse(hex, 16)
    <<int::128>>
  end

  defimpl String.Chars do
    def to_string(%SurrealDB.Uuid{value: value}), do: value
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(%SurrealDB.Uuid{value: value}, _opts) do
      concat(["#SurrealDB.Uuid<", value, ">"])
    end
  end
end
