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

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(%SurrealDB.Datetime{} = dt, _opts) do
      iso = dt |> SurrealDB.Datetime.to_datetime() |> DateTime.to_iso8601()
      concat(["#SurrealDB.Datetime<", iso, ">"])
    end
  end
end
