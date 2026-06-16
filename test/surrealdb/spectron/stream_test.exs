defmodule SurrealDB.Spectron.StreamTest do
  use ExUnit.Case, async: true

  import Plug.Conn

  alias SurrealDB.Spectron

  setup do
    bypass = Bypass.open()

    client =
      Spectron.new(
        endpoint: "http://localhost:#{bypass.port}",
        context: "acme",
        api_key: "secret",
        retry: false
      )

    {:ok, bypass: bypass, client: client}
  end

  test "chat with stream: true yields decoded chunks", %{bypass: bypass, client: client} do
    Bypass.expect_once(bypass, "POST", "/contexts/acme/chat", fn conn ->
      assert get_req_header(conn, "accept") == ["text/event-stream"]

      conn = send_chunked(conn, 200)
      {:ok, conn} = chunk(conn, ~s(data: {"delta":"He"}\n\n))
      {:ok, conn} = chunk(conn, ~s(data: {"delta":"llo"}\n\n))
      {:ok, conn} = chunk(conn, "data: [DONE]\n\n")
      conn
    end)

    assert {:ok, stream} = Spectron.chat(client, "hi", stream: true)
    chunks = Enum.to_list(stream)
    assert Enum.map(chunks, & &1["delta"]) == ["He", "llo"]
  end
end
