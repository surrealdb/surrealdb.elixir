defmodule SurrealDB.Transaction do
  @moduledoc """
  A handle to an interactive transaction, returned by `SurrealDB.begin/1`.

  Pass the handle wherever a connection is expected and the call runs inside the
  transaction. Nothing it writes is visible to other sessions until `SurrealDB.commit/1`;
  `SurrealDB.cancel/1` discards it. `SurrealDB.transaction/2` does both for you.

      {:ok, txn} = SurrealDB.begin(db)
      {:ok, _} = SurrealDB.create(txn, "account", %{balance: 100})
      {:ok, _} = SurrealDB.commit(txn)

  Transactions need a WebSocket connection and SurrealDB 3.0 or later. They do not survive
  a reconnect: a transaction open when the connection drops is gone, and calls with its
  handle fail.
  """

  @enforce_keys [:conn, :id]
  defstruct [:conn, :id, :session]

  @type t :: %__MODULE__{
          conn: GenServer.server(),
          id: SurrealDB.Uuid.t(),
          session: SurrealDB.Uuid.t() | nil
        }
end
