defmodule SurrealDB.RecordId do
  @moduledoc """
  A SurrealDB record identifier in the form `table:id`.

  The `id` part can be a string, integer, list, or map (object id). It is kept as a native
  Elixir term so it round-trips through both the CBOR and JSON protocols without loss.

      iex> SurrealDB.RecordId.new("person", "tobie")
      %SurrealDB.RecordId{table: "person", id: "tobie"}

      iex> SurrealDB.RecordId.parse("person:tobie")
      %SurrealDB.RecordId{table: "person", id: "tobie"}
  """

  @enforce_keys [:table, :id]
  defstruct [:table, :id]

  @type id :: String.t() | integer() | list() | map()
  @type t :: %__MODULE__{table: String.t(), id: id()}

  @doc "Builds a record id from a table name and an id part."
  @spec new(String.t() | SurrealDB.Table.t(), id()) :: t()
  def new(%SurrealDB.Table{name: table}, id), do: %__MODULE__{table: table, id: id}
  def new(table, id) when is_binary(table), do: %__MODULE__{table: table, id: id}

  @doc """
  Parses a `table:id` string. Only string ids are produced. For typed ids build the
  struct directly with `new/2`.
  """
  @spec parse(String.t()) :: t()
  def parse(string) when is_binary(string) do
    case String.split(string, ":", parts: 2) do
      [table, id] -> %__MODULE__{table: table, id: id}
      [table] -> %__MODULE__{table: table, id: nil}
    end
  end

  @doc "Returns the canonical `table:id` string representation."
  @spec to_string(t()) :: String.t()
  def to_string(%__MODULE__{table: table, id: id}) do
    "#{escape(table)}:#{format_id(id)}"
  end

  defp format_id(id) when is_binary(id), do: escape(id)
  defp format_id(id) when is_integer(id), do: Integer.to_string(id)
  defp format_id(id), do: inspect(id)

  # SurrealDB identifiers that are not simple alphanumerics are wrapped in backticks.
  defp escape(part) when is_binary(part) do
    if Regex.match?(~r/^[a-zA-Z_][a-zA-Z0-9_]*$/, part) do
      part
    else
      "`" <> String.replace(part, "`", "\\`") <> "`"
    end
  end

  defimpl String.Chars do
    def to_string(record_id), do: SurrealDB.RecordId.to_string(record_id)
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(record_id, _opts) do
      concat(["#SurrealDB.RecordId<", SurrealDB.RecordId.to_string(record_id), ">"])
    end
  end
end
