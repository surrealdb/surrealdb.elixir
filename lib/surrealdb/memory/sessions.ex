defmodule SurrealDB.Memory.Sessions do
  @moduledoc """
  Session operations for a `SurrealDB.Memory` client.

  `create/2` returns a `SurrealDB.Memory.Session` handle you can record turns against,
  fetch context for, and close.

      {:ok, session} = SurrealDB.Memory.Sessions.create(client)
      {:ok, _} = SurrealDB.Memory.Session.turns(session, [%{role: "user", content: "hi"}])
      {:ok, ctx} = SurrealDB.Memory.Session.context(session)
      {:ok, _} = SurrealDB.Memory.Session.close(session)
  """

  alias SurrealDB.Memory
  alias SurrealDB.Memory.Session

  @doc "Creates a session and returns a `SurrealDB.Memory.Session` handle."
  @spec create(Memory.t(), keyword()) :: {:ok, Session.t()} | Memory.result()
  def create(client, opts \\ []) do
    case Memory.post(client, "/sessions", Memory.body(opts)) do
      {:ok, %{"id" => id} = response} -> {:ok, %Session{client: client, id: id, data: response}}
      other -> other
    end
  end
end
