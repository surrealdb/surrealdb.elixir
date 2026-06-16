defmodule SurrealDB.Spectron.Scopes do
  @moduledoc """
  Scope operations for a `SurrealDB.Spectron` client.
  """

  alias SurrealDB.Spectron

  @doc "Lists scopes."
  @spec list(Spectron.t(), keyword()) :: Spectron.result()
  def list(client, opts \\ []), do: Spectron.get(client, "/scopes", query: opts)

  @doc "Registers a new scope."
  @spec register(Spectron.t(), keyword()) :: Spectron.result()
  def register(client, opts), do: Spectron.post(client, "/scopes", Spectron.body(opts))

  @doc "Deletes a scope."
  @spec delete(Spectron.t(), String.t()) :: Spectron.result()
  def delete(client, scope), do: Spectron.delete(client, "/scopes/#{seg(scope)}")

  @doc "Forgets all memory under a scope."
  @spec forget(Spectron.t(), String.t(), keyword()) :: Spectron.result()
  def forget(client, scope, opts \\ []) do
    Spectron.post(client, "/scopes/#{seg(scope)}/forget", Spectron.body(opts))
  end

  defp seg(value), do: URI.encode(to_string(value), &URI.char_unreserved?/1)
end
