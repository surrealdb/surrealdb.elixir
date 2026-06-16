defmodule SurrealDB.Spectron.Keys do
  @moduledoc """
  Self-service API key operations for a `SurrealDB.Spectron` client.

      {:ok, minted} = SurrealDB.Spectron.Keys.create(client, name: "ci", ttl_seconds: 3600)
  """

  alias SurrealDB.Spectron

  @doc "Mints a new API key."
  @spec create(Spectron.t(), keyword()) :: Spectron.result()
  def create(client, opts), do: Spectron.post(client, "/keys", Spectron.body(opts))

  @doc "Lists API keys."
  @spec list(Spectron.t(), keyword()) :: Spectron.result()
  def list(client, opts \\ []), do: Spectron.get(client, "/keys", query: opts)

  @doc "Deletes an API key by name."
  @spec delete(Spectron.t(), String.t()) :: Spectron.result()
  def delete(client, name), do: Spectron.delete(client, "/keys/#{seg(name)}")

  @doc "Rotates an API key by name and returns the new secret."
  @spec rotate(Spectron.t(), String.t()) :: Spectron.result()
  def rotate(client, name), do: Spectron.post(client, "/keys/#{seg(name)}/rotate", %{})

  defp seg(value), do: URI.encode(to_string(value), &URI.char_unreserved?/1)
end
