defmodule SurrealDB.Spectron.Session do
  @moduledoc """
  A handle to a Spectron session, returned by `SurrealDB.Spectron.Sessions.create/2`.
  """

  alias SurrealDB.Spectron

  @enforce_keys [:client, :id]
  defstruct [:client, :id, data: %{}]

  @type t :: %__MODULE__{client: Spectron.t(), id: String.t(), data: map()}

  @doc "Records one or more conversation turns in the session."
  @spec turns(t(), [map()]) :: Spectron.result()
  def turns(%__MODULE__{client: client, id: id}, turns) do
    Spectron.post(client, "/sessions/#{seg(id)}/turns", %{"turns" => turns})
  end

  @doc "Returns the working context accumulated in the session."
  @spec context(t(), keyword()) :: Spectron.result()
  def context(%__MODULE__{client: client, id: id}, opts \\ []) do
    Spectron.get(client, "/sessions/#{seg(id)}/context", query: opts)
  end

  @doc "Closes the session."
  @spec close(t()) :: Spectron.result()
  def close(%__MODULE__{client: client, id: id}) do
    Spectron.post(client, "/sessions/#{seg(id)}/close", %{})
  end

  defp seg(value), do: URI.encode(to_string(value), &URI.char_unreserved?/1)
end
