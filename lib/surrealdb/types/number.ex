defmodule SurrealDB.Number do
  @moduledoc """
  A number with a fixed size: one of SurrealDB's sized number kinds.

  SurrealDB 4.0 can store a number as a sized kind, such as an unsigned 8-bit integer
  (`5u8` in SurrealQL) or a 32-bit float (`1.5f32`). This struct keeps that kind. A plain
  Elixir integer or float stays a plain SurrealDB `int` or `float`.

  | Kind   | Range or precision           | CBOR tag (`cbor.2`) |
  | ------ | ---------------------------- | ------------------- |
  | `:i8`  | -128 to 127                  | 180                 |
  | `:u8`  | 0 to 255                     | 181                 |
  | `:i16` | -32768 to 32767              | 182                 |
  | `:u16` | 0 to 65535                   | 183                 |
  | `:i32` | -2147483648 to 2147483647    | 184                 |
  | `:u32` | 0 to 4294967295              | 185                 |
  | `:u64` | 0 to 18446744073709551615    | 186                 |
  | `:f16` | IEEE 754 half precision      | 187                 |
  | `:f32` | IEEE 754 single precision    | 188                 |

  The kinds travel only over the `cbor.2` protocol, which the SDK negotiates with
  SurrealDB 4.0 and later. Over plain `cbor` (older servers) and JSON a sized number is
  sent as a plain number, and a `:u64` above the 64-bit signed limit is sent as a decimal.

  A float kind holds a float, or `:infinity`, `:neg_infinity` or `:nan`. Building one rounds
  the value to its precision, as a cast in SurrealQL does.

      iex> SurrealDB.Number.u8(5)
      %SurrealDB.Number{kind: :u8, value: 5}

      iex> SurrealDB.Number.f32(0.1)
      %SurrealDB.Number{kind: :f32, value: 0.10000000149011612}
  """

  @enforce_keys [:kind, :value]
  defstruct [:kind, :value]

  @type kind :: :i8 | :u8 | :i16 | :u16 | :i32 | :u32 | :u64 | :f16 | :f32
  @type t :: %__MODULE__{
          kind: kind(),
          value: integer() | float() | :infinity | :neg_infinity | :nan
        }

  @ranges %{
    i8: -0x80..0x7F,
    u8: 0..0xFF,
    i16: -0x8000..0x7FFF,
    u16: 0..0xFFFF,
    i32: -0x80000000..0x7FFFFFFF,
    u32: 0..0xFFFFFFFF,
    u64: 0..0xFFFFFFFFFFFFFFFF
  }

  @integer_kinds Map.keys(@ranges)
  @float_kinds [:f16, :f32]

  @doc "Every sized kind, in tag order."
  @spec kinds() :: [kind()]
  def kinds, do: [:i8, :u8, :i16, :u16, :i32, :u32, :u64, :f16, :f32]

  @doc """
  Builds a sized number, raising `ArgumentError` when the value does not fit the kind.

      iex> SurrealDB.Number.new(:i16, -300)
      %SurrealDB.Number{kind: :i16, value: -300}
  """
  @spec new(kind(), number() | :infinity | :neg_infinity | :nan) :: t()
  def new(kind, value) when kind in @integer_kinds and is_integer(value) do
    if value in Map.fetch!(@ranges, kind) do
      %__MODULE__{kind: kind, value: value}
    else
      raise ArgumentError, "#{value} does not fit in #{kind}"
    end
  end

  def new(kind, value) when kind in @float_kinds and value in [:infinity, :neg_infinity, :nan],
    do: %__MODULE__{kind: kind, value: value}

  def new(kind, value) when kind in @float_kinds and is_number(value) do
    rounded = round_to(kind, value / 1)

    if rounded in [:infinity, :neg_infinity] do
      raise ArgumentError, "#{value} is too large for #{kind}"
    end

    %__MODULE__{kind: kind, value: rounded}
  end

  def new(kind, value) do
    raise ArgumentError, "cannot build a #{inspect(kind)} number from #{inspect(value)}"
  end

  for kind <- [:i8, :u8, :i16, :u16, :i32, :u32, :u64, :f16, :f32] do
    @doc "Builds a `#{kind}`. See `new/2`."
    @spec unquote(kind)(number() | :infinity | :neg_infinity | :nan) :: t()
    def unquote(kind)(value), do: new(unquote(kind), value)
  end

  @doc """
  Returns the plain Elixir value: an integer, a float, or a special float atom.

      iex> SurrealDB.Number.to_plain(SurrealDB.Number.u8(5))
      5
  """
  @spec to_plain(t()) :: integer() | float() | :infinity | :neg_infinity | :nan
  def to_plain(%__MODULE__{value: value}), do: value

  @doc false
  # Rounds a float to the precision of `kind`, giving the special atoms for the values the
  # BEAM cannot hold.
  def round_to(:f16, float), do: float |> then(&<<&1::float-16>>) |> read_float()
  def round_to(:f32, float), do: float |> then(&<<&1::float-32>>) |> read_float()

  @doc false
  # Whether `float` is exactly representable at the precision of `kind`.
  def exact?(_kind, special) when special in [:infinity, :neg_infinity, :nan], do: true
  def exact?(kind, float) when is_float(float), do: round_to(kind, float) === float
  def exact?(_kind, _value), do: false

  defp read_float(<<sign::1, 0x1F::5, mantissa::10>>), do: special(sign, mantissa)
  defp read_float(<<sign::1, 0xFF::8, mantissa::23>>), do: special(sign, mantissa)
  defp read_float(<<float::float-16>>), do: float
  defp read_float(<<float::float-32>>), do: float

  defp special(_sign, mantissa) when mantissa != 0, do: :nan
  defp special(0, 0), do: :infinity
  defp special(1, 0), do: :neg_infinity

  defimpl String.Chars do
    def to_string(number), do: SurrealDB.SurrealQL.to_surql(number)
  end
end
