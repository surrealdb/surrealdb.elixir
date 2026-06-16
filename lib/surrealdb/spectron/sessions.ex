defmodule SurrealDB.Spectron.Sessions do
  @moduledoc """
  Session operations for a `SurrealDB.Spectron` client.

  `create/2` returns a `SurrealDB.Spectron.Session` handle you can record turns against,
  fetch context for, and close.

      {:ok, session} = SurrealDB.Spectron.Sessions.create(client)
      {:ok, _} = SurrealDB.Spectron.Session.turns(session, [%{role: "user", content: "hi"}])
      {:ok, ctx} = SurrealDB.Spectron.Session.context(session)
      {:ok, _} = SurrealDB.Spectron.Session.close(session)
  """

  alias SurrealDB.Spectron
  alias SurrealDB.Spectron.Session

  @doc "Creates a session and returns a `SurrealDB.Spectron.Session` handle."
  @spec create(Spectron.t(), keyword()) :: {:ok, Session.t()} | Spectron.result()
  def create(client, opts \\ []) do
    case Spectron.post(client, "/sessions", Spectron.body(opts)) do
      {:ok, %{"id" => id} = response} -> {:ok, %Session{client: client, id: id, data: response}}
      other -> other
    end
  end
end
