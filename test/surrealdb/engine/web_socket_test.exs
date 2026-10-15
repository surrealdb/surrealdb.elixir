defmodule SurrealDB.Engine.WebSocketTest do
  use ExUnit.Case, async: true

  alias SurrealDB.Engine.WebSocket

  test "closes the socket when the server refuses the upgrade" do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    parent = self()

    spawn_link(fn ->
      {:ok, socket} = :gen_tcp.accept(listen)
      {:ok, _request} = :gen_tcp.recv(socket, 0, 5_000)
      :ok = :gen_tcp.send(socket, "HTTP/1.1 403 Forbidden\r\ncontent-length: 0\r\n\r\n")
      send(parent, {:after_refusal, :gen_tcp.recv(socket, 0, 5_000)})
    end)

    uri = URI.parse("ws://127.0.0.1:#{port}/rpc")
    assert {:error, _reason} = WebSocket.connect(uri, [])
    assert_receive {:after_refusal, {:error, :closed}}, 5_000
  end
end
