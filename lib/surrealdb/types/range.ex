defmodule SurrealDB.Range do
  @moduledoc """
  A SurrealDB range value, used for record id ranges and value ranges.

  Each bound is one of:

    * `{:incl, value}` - inclusive bound
    * `{:excl, value}` - exclusive bound
    * `nil` - unbounded

  Over CBOR a range is tag 49 holding the two bounds, each wrapped in tag 50 (inclusive)
  or tag 51 (exclusive).

      iex> SurrealDB.Range.new({:incl, 1}, {:excl, 10})
      %SurrealDB.Range{begin: {:incl, 1}, end: {:excl, 10}}
  """

  defstruct begin: nil, end: nil

  @type bound :: {:incl, term()} | {:excl, term()} | nil
  @type t :: %__MODULE__{begin: bound(), end: bound()}

  @doc "Builds a range from a begin and end bound."
  @spec new(bound(), bound()) :: t()
  def new(begin_bound \\ nil, end_bound \\ nil) do
    %__MODULE__{begin: begin_bound, end: end_bound}
  end
end
