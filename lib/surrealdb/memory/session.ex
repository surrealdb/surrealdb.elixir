defmodule SurrealDB.Memory.Session do
  @moduledoc """
  A handle to an Agent Memory session, returned by `SurrealDB.Memory.Sessions.create/2`.
  """

  alias SurrealDB.Memory

  @enforce_keys [:client, :id]
  defstruct [:client, :id, data: %{}]

  @type t :: %__MODULE__{client: Memory.t(), id: String.t(), data: map()}

  @doc "Records one or more conversation turns in the session."
  @spec turns(t(), [map()]) :: Memory.result()
  def turns(%__MODULE__{client: client, id: id}, turns) do
    Memory.post(client, "/sessions/#{seg(id)}/turns", %{"turns" => turns})
  end

  @doc "Returns the working context accumulated in the session."
  @spec context(t(), keyword()) :: Memory.result()
  def context(%__MODULE__{client: client, id: id}, opts \\ []) do
    Memory.get(client, "/sessions/#{seg(id)}/context", query: opts)
  end

  @doc "Closes the session."
  @spec close(t()) :: Memory.result()
  def close(%__MODULE__{client: client, id: id}) do
    Memory.post(client, "/sessions/#{seg(id)}/close", %{})
  end

  defp seg(value), do: URI.encode(to_string(value), &URI.char_unreserved?/1)
end
