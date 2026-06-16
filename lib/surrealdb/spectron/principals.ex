defmodule SurrealDB.Spectron.Principals do
  @moduledoc """
  Principal and authorisation operations for a `SurrealDB.Spectron` client.
  """

  alias SurrealDB.Spectron

  @doc "Lists principals."
  @spec list(Spectron.t(), keyword()) :: Spectron.result()
  def list(client, opts \\ []), do: Spectron.get(client, "/principals", query: opts)

  @doc "Returns a single principal."
  @spec get(Spectron.t(), String.t()) :: Spectron.result()
  def get(client, principal_id), do: Spectron.get(client, "/principals/#{seg(principal_id)}")

  @doc "Returns the effective grants for a principal."
  @spec effective(Spectron.t(), String.t()) :: Spectron.result()
  def effective(client, principal_id) do
    Spectron.get(client, "/principals/#{seg(principal_id)}/effective")
  end

  @doc "Grants a permission to a principal."
  @spec grant(Spectron.t(), String.t(), keyword()) :: Spectron.result()
  def grant(client, principal_id, opts) do
    Spectron.post(client, "/principals/#{seg(principal_id)}/grant", Spectron.body(opts))
  end

  @doc "Revokes a permission from a principal."
  @spec revoke(Spectron.t(), String.t(), keyword()) :: Spectron.result()
  def revoke(client, principal_id, opts) do
    Spectron.post(client, "/principals/#{seg(principal_id)}/revoke", Spectron.body(opts))
  end

  defp seg(value), do: URI.encode(to_string(value), &URI.char_unreserved?/1)
end
