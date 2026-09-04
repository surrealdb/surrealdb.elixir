defmodule SurrealDB.Memory.Keys do
  @moduledoc """
  Self-service API key operations for a `SurrealDB.Memory` client.

      {:ok, minted} = SurrealDB.Memory.Keys.create(client, name: "ci", ttl_seconds: 3600)
  """

  alias SurrealDB.Memory

  @doc "Mints a new API key."
  @spec create(Memory.t(), keyword()) :: Memory.result()
  def create(client, opts), do: Memory.post(client, "/keys", Memory.body(opts))

  @doc "Lists API keys."
  @spec list(Memory.t(), keyword()) :: Memory.result()
  def list(client, opts \\ []), do: Memory.get(client, "/keys", query: opts)

  @doc "Deletes an API key by name."
  @spec delete(Memory.t(), String.t()) :: Memory.result()
  def delete(client, name), do: Memory.delete(client, "/keys/#{seg(name)}")

  @doc "Rotates an API key by name and returns the new secret."
  @spec rotate(Memory.t(), String.t()) :: Memory.result()
  def rotate(client, name), do: Memory.post(client, "/keys/#{seg(name)}/rotate", %{})

  defp seg(value), do: URI.encode(to_string(value), &URI.char_unreserved?/1)
end
