defmodule SurrealDB.Session do
  @moduledoc """
  A handle to a session on a connection, returned by `SurrealDB.new_session/1` and
  `SurrealDB.fork_session/1`.

  A session has its own namespace, database, authentication, and parameters, and shares the
  connection with the connection's default session and any other sessions. Pass the handle
  wherever a connection is expected and the call runs in that session:

      {:ok, session} = SurrealDB.new_session(db)
      {:ok, _} = SurrealDB.use(session, namespace: "app", database: "tenant_a")
      {:ok, rows} = SurrealDB.select(session, "person")
      {:ok, _} = SurrealDB.close_session(session)

  Sessions need a WebSocket connection and SurrealDB 3.0 or later.
  """

  @enforce_keys [:conn, :id]
  defstruct [:conn, :id]

  @type t :: %__MODULE__{conn: GenServer.server(), id: SurrealDB.Uuid.t()}
end
