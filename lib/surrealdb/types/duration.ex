defmodule SurrealDB.Duration do
  @moduledoc """
  A SurrealDB duration, stored internally as a whole number of nanoseconds.

  Over CBOR it is encoded as a compact `[seconds, nanoseconds]` pair (tag 14).

      iex> SurrealDB.Duration.new(1500, :millisecond)
      %SurrealDB.Duration{nanoseconds: 1_500_000_000}

      iex> SurrealDB.Duration.parse("1h30m")
      %SurrealDB.Duration{nanoseconds: 5_400_000_000_000}
  """

  @enforce_keys [:nanoseconds]
  defstruct [:nanoseconds]

  @type t :: %__MODULE__{nanoseconds: non_neg_integer()}
  @type unit ::
          :nanosecond
          | :microsecond
          | :millisecond
          | :second
          | :minute
          | :hour
          | :day
          | :week
          | :year

  @units %{
    nanosecond: 1,
    microsecond: 1_000,
    millisecond: 1_000_000,
    second: 1_000_000_000,
    minute: 60 * 1_000_000_000,
    hour: 3_600 * 1_000_000_000,
    day: 86_400 * 1_000_000_000,
    week: 7 * 86_400 * 1_000_000_000,
    year: 365 * 86_400 * 1_000_000_000
  }

  @format_units [
    {"y", 365 * 86_400 * 1_000_000_000},
    {"w", 7 * 86_400 * 1_000_000_000},
    {"d", 86_400 * 1_000_000_000},
    {"h", 3_600 * 1_000_000_000},
    {"m", 60 * 1_000_000_000},
    {"s", 1_000_000_000},
    {"ms", 1_000_000},
    {"µs", 1_000},
    {"ns", 1}
  ]

  @doc "Builds a duration from an amount and unit (defaults to nanoseconds)."
  @spec new(integer(), unit()) :: t()
  def new(amount, unit \\ :nanosecond) when is_integer(amount) do
    %__MODULE__{nanoseconds: amount * Map.fetch!(@units, unit)}
  end

  @doc "Builds a duration directly from a `{seconds, nanoseconds}` pair."
  @spec from_parts(integer(), integer()) :: t()
  def from_parts(seconds, nanos) do
    %__MODULE__{nanoseconds: seconds * 1_000_000_000 + nanos}
  end

  @doc "Returns the `{seconds, nanoseconds}` pair used by the CBOR encoding."
  @spec to_parts(t()) :: {non_neg_integer(), non_neg_integer()}
  def to_parts(%__MODULE__{nanoseconds: ns}), do: {div(ns, 1_000_000_000), rem(ns, 1_000_000_000)}

  @doc """
  Parses a SurrealDB duration string such as `"1y"`, `"2w"`, `"1h30m"`, or `"500ms"`.
  Supported suffixes: `ns`, `us`/`µs`, `ms`, `s`, `m`, `h`, `d`, `w`, `y`. A year is 365
  days, as in SurrealDB.
  """
  @spec parse(String.t()) :: t()
  def parse(string) when is_binary(string) do
    nanos =
      ~r/([0-9]+)(ns|us|µs|ms|s|m|h|d|w|y)/u
      |> Regex.scan(string)
      |> Enum.reduce(0, fn [_, amount, suffix], acc ->
        acc + String.to_integer(amount) * suffix_to_nanos(suffix)
      end)

    %__MODULE__{nanoseconds: nanos}
  end

  defp suffix_to_nanos("ns"), do: @units.nanosecond
  defp suffix_to_nanos("us"), do: @units.microsecond
  defp suffix_to_nanos("µs"), do: @units.microsecond
  defp suffix_to_nanos("ms"), do: @units.millisecond
  defp suffix_to_nanos("s"), do: @units.second
  defp suffix_to_nanos("m"), do: @units.minute
  defp suffix_to_nanos("h"), do: @units.hour
  defp suffix_to_nanos("d"), do: @units.day
  defp suffix_to_nanos("w"), do: @units.week
  defp suffix_to_nanos("y"), do: @units.year

  @doc """
  Formats the duration as SurrealDB does, for example `"1h30m"` or `"0ns"`. A year is 365
  days.
  """
  @spec to_string(t()) :: String.t()
  def to_string(%__MODULE__{nanoseconds: 0}), do: "0ns"

  def to_string(%__MODULE__{nanoseconds: ns}) do
    {parts, _rest} =
      Enum.reduce(@format_units, {[], ns}, fn {suffix, size}, {parts, rest} ->
        case div(rest, size) do
          0 -> {parts, rest}
          count -> {[Integer.to_string(count) <> suffix | parts], rem(rest, size)}
        end
      end)

    parts |> Enum.reverse() |> Enum.join()
  end

  defimpl String.Chars do
    def to_string(duration), do: SurrealDB.Duration.to_string(duration)
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(%SurrealDB.Duration{} = duration, _opts) do
      concat(["#SurrealDB.Duration<", SurrealDB.Duration.to_string(duration), ">"])
    end
  end
end
