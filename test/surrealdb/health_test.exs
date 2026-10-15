defmodule SurrealDB.HealthTest do
  use ExUnit.Case, async: true

  test "a health check that times out closes its connection" do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    parent = self()

    spawn_link(fn ->
      {:ok, socket} = :gen_tcp.accept(listen)
      {:ok, _request} = :gen_tcp.recv(socket, 0, 5_000)
      # Never answer; report when the client hangs up.
      send(parent, {:client, :gen_tcp.recv(socket, 0, 5_000)})
    end)

    session = %{uri: URI.parse("http://127.0.0.1:#{port}/rpc")}
    assert {:error, %SurrealDB.Error{}} = SurrealDB.Rest.health(session, timeout: 200)
    assert_receive {:client, {:error, :closed}}, 5_000
  end
end
