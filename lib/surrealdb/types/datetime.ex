defmodule SurrealDB.Datetime do
  @moduledoc """
  A SurrealDB datetime with nanosecond precision, stored as `{seconds, nanoseconds}` since
  the Unix epoch.

  Over CBOR it is encoded as a compact `[seconds, nanoseconds]` pair (tag 12). The codec
  also accepts a native `DateTime` so you rarely need to build this struct by hand.

      iex> SurrealDB.Datetime.from_unix(1_700_000_000, 500)
      %SurrealDB.Datetime{seconds: 1_700_000_000, nanos: 500}
  """

  @enforce_keys [:seconds, :nanos]
  defstruct [:seconds, :nanos]

  @type t :: %__MODULE__{seconds: integer(), nanos: non_neg_integer()}

  @doc "Builds a datetime from Unix seconds and a nanosecond fraction."
  @spec from_unix(integer(), non_neg_integer()) :: t()
  def from_unix(seconds, nanos \\ 0), do: %__MODULE__{seconds: seconds, nanos: nanos}

  @doc "Builds a datetime from a native `DateTime`."
  @spec from_datetime(DateTime.t()) :: t()
  def from_datetime(%DateTime{} = dt) do
    nanos = DateTime.to_unix(dt, :nanosecond)
    %__MODULE__{seconds: div(nanos, 1_000_000_000), nanos: rem(nanos, 1_000_000_000)}
  end

  @doc """
  Converts to a native `DateTime`. Precision is truncated to microseconds, the finest
  resolution Elixir's `DateTime` supports.
  """
  @spec to_datetime(t()) :: DateTime.t()
  def to_datetime(%__MODULE__{seconds: seconds, nanos: nanos}) do
    DateTime.from_unix!(seconds * 1_000_000_000 + nanos, :nanosecond)
  end

  @doc """
  Formats as an RFC 3339 string in UTC with full nanosecond precision, dropping trailing
  zeros from the fraction.
  """
  @spec to_iso8601(t()) :: String.t()
  def to_iso8601(%__MODULE__{seconds: seconds, nanos: nanos}) do
    base = seconds |> DateTime.from_unix!() |> DateTime.to_iso8601() |> String.trim_trailing("Z")

    fraction =
      case nanos do
        0 ->
          ""

        _ ->
          "." <>
            (nanos
             |> Integer.to_string()
             |> String.pad_leading(9, "0")
             |> String.trim_trailing("0"))
      end

    base <> fraction <> "Z"
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(%SurrealDB.Datetime{} = dt, _opts) do
      iso = dt |> SurrealDB.Datetime.to_datetime() |> DateTime.to_iso8601()
      concat(["#SurrealDB.Datetime<", iso, ">"])
    end
  end
end
