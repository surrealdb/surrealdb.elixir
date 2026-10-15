defmodule SurrealDB.StringRecordId do
  @moduledoc """
  A record id written as a SurrealQL string, for the server to parse.

  The SDK never treats a plain string as a record id. When you hold a record id as text,
  wrap it in a `StringRecordId` to send it as one: over CBOR it is a record id tag holding
  the text, and the server parses it with the full SurrealQL grammar, so every key type
  works.

      SurrealDB.select(db, SurrealDB.StringRecordId.new("person:tobie"))
      SurrealDB.select(db, SurrealDB.StringRecordId.new("temp:['london', 2024]"))

  To parse on the client instead, use `SurrealDB.RecordId.parse/1`.
  """

  @enforce_keys [:id]
  defstruct [:id]

  @type t :: %__MODULE__{id: String.t()}

  @doc "Wraps a SurrealQL record id string."
  @spec new(String.t()) :: t()
  def new(id) when is_binary(id), do: %__MODULE__{id: id}

  defimpl String.Chars do
    def to_string(%SurrealDB.StringRecordId{id: id}), do: id
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(%SurrealDB.StringRecordId{id: id}, _opts),
      do: concat(["#SurrealDB.StringRecordId<", id, ">"])
  end
end
