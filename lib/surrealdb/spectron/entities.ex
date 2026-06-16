defmodule SurrealDB.Spectron.Entities do
  @moduledoc """
  Entity operations for a `SurrealDB.Spectron` client. Each function takes the client first.
  """

  alias SurrealDB.Spectron

  @doc "Lists entities."
  @spec list(Spectron.t(), keyword()) :: Spectron.result()
  def list(client, opts \\ []), do: Spectron.get(client, "/entities", query: opts)

  @doc "Returns a single entity."
  @spec get(Spectron.t(), String.t()) :: Spectron.result()
  def get(client, entity_ref), do: Spectron.get(client, "/entities/#{seg(entity_ref)}")

  @doc "Returns the change history of an entity."
  @spec history(Spectron.t(), String.t(), keyword()) :: Spectron.result()
  def history(client, entity_ref, opts \\ []) do
    Spectron.get(client, "/entities/#{seg(entity_ref)}/history", query: opts)
  end

  @doc "Deletes an entity."
  @spec delete(Spectron.t(), String.t()) :: Spectron.result()
  def delete(client, entity_ref), do: Spectron.delete(client, "/entities/#{seg(entity_ref)}")

  defp seg(value), do: URI.encode(to_string(value), &URI.char_unreserved?/1)
end
