defmodule SurrealDB.Table do
  @moduledoc """
  A SurrealDB table name.

  Wrapping a bare string in a `Table` lets the protocol encode it as a table reference
  rather than a plain string, which matters for methods such as `create` and `select`
  that accept either a whole table or a single record.

      iex> SurrealDB.Table.new("person")
      %SurrealDB.Table{name: "person"}
  """

  @enforce_keys [:name]
  defstruct [:name]

  @type t :: %__MODULE__{name: String.t()}

  @doc "Builds a table reference from a name."
  @spec new(String.t()) :: t()
  def new(name) when is_binary(name), do: %__MODULE__{name: name}

  defimpl String.Chars do
    def to_string(%SurrealDB.Table{name: name}), do: name
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(%SurrealDB.Table{name: name}, _opts) do
      concat(["#SurrealDB.Table<", name, ">"])
    end
  end
end
