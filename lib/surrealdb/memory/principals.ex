defmodule SurrealDB.Memory.Principals do
  @moduledoc """
  Principal and authorisation operations for a `SurrealDB.Memory` client.
  """

  alias SurrealDB.Memory

  @doc "Lists principals."
  @spec list(Memory.t(), keyword()) :: Memory.result()
  def list(client, opts \\ []), do: Memory.get(client, "/principals", query: opts)

  @doc "Returns a single principal."
  @spec get(Memory.t(), String.t()) :: Memory.result()
  def get(client, principal_id), do: Memory.get(client, "/principals/#{seg(principal_id)}")

  @doc "Returns the effective grants for a principal."
  @spec effective(Memory.t(), String.t()) :: Memory.result()
  def effective(client, principal_id) do
    Memory.get(client, "/principals/#{seg(principal_id)}/effective")
  end

  @doc "Grants a permission to a principal."
  @spec grant(Memory.t(), String.t(), keyword()) :: Memory.result()
  def grant(client, principal_id, opts) do
    Memory.post(client, "/principals/#{seg(principal_id)}/grant", Memory.body(opts))
  end

  @doc "Revokes a permission from a principal."
  @spec revoke(Memory.t(), String.t(), keyword()) :: Memory.result()
  def revoke(client, principal_id, opts) do
    Memory.post(client, "/principals/#{seg(principal_id)}/revoke", Memory.body(opts))
  end

  defp seg(value), do: URI.encode(to_string(value), &URI.char_unreserved?/1)
end
