defmodule SurrealDB.Memory.Scopes do
  @moduledoc """
  Scope operations for a `SurrealDB.Memory` client.
  """

  alias SurrealDB.Memory

  @doc "Lists scopes."
  @spec list(Memory.t(), keyword()) :: Memory.result()
  def list(client, opts \\ []), do: Memory.get(client, "/scopes", query: opts)

  @doc "Registers a new scope."
  @spec register(Memory.t(), keyword()) :: Memory.result()
  def register(client, opts), do: Memory.post(client, "/scopes", Memory.body(opts))

  @doc "Deletes a scope."
  @spec delete(Memory.t(), String.t()) :: Memory.result()
  def delete(client, scope), do: Memory.delete(client, "/scopes/#{seg(scope)}")

  @doc "Forgets all memory under a scope."
  @spec forget(Memory.t(), String.t(), keyword()) :: Memory.result()
  def forget(client, scope, opts \\ []) do
    Memory.post(client, "/scopes/#{seg(scope)}/forget", Memory.body(opts))
  end

  defp seg(value), do: URI.encode(to_string(value), &URI.char_unreserved?/1)
end
