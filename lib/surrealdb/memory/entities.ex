defmodule SurrealDB.Memory.Entities do
  @moduledoc """
  Entity operations for a `SurrealDB.Memory` client. Each function takes the client first.
  """

  alias SurrealDB.Memory

  @doc "Lists entities."
  @spec list(Memory.t(), keyword()) :: Memory.result()
  def list(client, opts \\ []), do: Memory.get(client, "/entities", query: opts)

  @doc "Returns a single entity."
  @spec get(Memory.t(), String.t()) :: Memory.result()
  def get(client, entity_ref), do: Memory.get(client, "/entities/#{seg(entity_ref)}")

  @doc "Returns the change history of an entity."
  @spec history(Memory.t(), String.t(), keyword()) :: Memory.result()
  def history(client, entity_ref, opts \\ []) do
    Memory.get(client, "/entities/#{seg(entity_ref)}/history", query: opts)
  end

  @doc "Deletes an entity."
  @spec delete(Memory.t(), String.t()) :: Memory.result()
  def delete(client, entity_ref), do: Memory.delete(client, "/entities/#{seg(entity_ref)}")

  defp seg(value), do: URI.encode(to_string(value), &URI.char_unreserved?/1)
end
