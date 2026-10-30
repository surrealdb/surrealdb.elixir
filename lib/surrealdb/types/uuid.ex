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

  @doc "Generates a random (version 4) UUID."
  @spec v4() :: t()
  def v4 do
    <<a::48, _::4, b::12, _::2, c::62>> = :crypto.strong_rand_bytes(16)
    from_binary(<<a::48, 4::4, b::12, 2::2, c::62>>)
  end

  @doc """
  Generates a time-ordered (version 7) UUID: the current Unix time in milliseconds and a
  counter, followed by random bits. Each UUID sorts after every one generated before it on
  this node, including within the same millisecond.
  """
  @spec v7() :: t()
  def v7 do
    <<millis::48, counter::12>> = <<next_stamp()::60>>
    <<c::62, _::2>> = :crypto.strong_rand_bytes(8)
    from_binary(<<millis::48, 7::4, counter::12, 2::2, c::62>>)
  end

  # The time in milliseconds times 4096, plus a 12-bit counter in the low bits: at least
  # one more than the last stamp handed out, so stamps only go up. A counter that fills up
  # carries into the time, as RFC 9562 allows.
  defp next_stamp do
    clock = clock()
    last = :atomics.get(clock, 1)
    next = max(System.system_time(:millisecond) * 4096, last + 1)

    case :atomics.compare_exchange(clock, 1, last, next) do
      :ok -> next
      _changed -> next_stamp()
    end
  end

  @clock {__MODULE__, :v7_clock}

  defp clock do
    case :persistent_term.get(@clock, nil) do
      nil -> :global.trans({@clock, self()}, &create_clock/0, [node()])
      clock -> clock
    end
  end

  defp create_clock do
    case :persistent_term.get(@clock, nil) do
      nil ->
        clock = :atomics.new(1, signed: false)
        :persistent_term.put(@clock, clock)
        clock

      clock ->
        clock
    end
  end

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
