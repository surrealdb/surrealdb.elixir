defmodule SurrealDB.RecordIdRange do
  @moduledoc """
  A range of record ids in one table, such as `person:1..=100` in SurrealQL.

  Each bound is `{:incl, key}`, `{:excl, key}`, or `nil` for unbounded, where `key` is a
  record id key (a string, integer, list, map, or `SurrealDB.Uuid`). Pass one to
  `SurrealDB.select/2` and friends to act on every record in the range.

      iex> SurrealDB.RecordIdRange.new("person", {:incl, 1}, {:incl, 100})
      %SurrealDB.RecordIdRange{table: "person", begin: {:incl, 1}, end: {:incl, 100}}

  Over CBOR it is a record id (tag 8) whose key is a range (tag 49). JSON cannot carry
  one, so a JSON connection refuses it with an `:unsupported` error.
  """

  @enforce_keys [:table]
  defstruct [:table, begin: nil, end: nil]

  @type t :: %__MODULE__{
          table: String.t(),
          begin: SurrealDB.Range.bound(),
          end: SurrealDB.Range.bound()
        }

  @doc "Builds a record id range from a table and its begin and end bounds."
  @spec new(String.t() | SurrealDB.Table.t(), SurrealDB.Range.bound(), SurrealDB.Range.bound()) ::
          t()
  def new(table, begin_bound \\ nil, end_bound \\ nil)

  def new(%SurrealDB.Table{name: table}, begin_bound, end_bound),
    do: new(table, begin_bound, end_bound)

  def new(table, begin_bound, end_bound) when is_binary(table),
    do: %__MODULE__{table: table, begin: begin_bound, end: end_bound}
end
