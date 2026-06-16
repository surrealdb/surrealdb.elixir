defmodule SurrealDB.RPCTest do
  use ExUnit.Case, async: true

  alias SurrealDB.{Engine, RPC}

  test "builds a request map" do
    assert RPC.request(1, "query", ["SELECT 1", %{}]) ==
             %{"id" => 1, "method" => "query", "params" => ["SELECT 1", %{}]}
  end

  describe "parse_query/1" do
    test "extracts results from OK statements" do
      statements = [
        %{"status" => "OK", "time" => "1ms", "result" => [%{"n" => 1}]},
        %{"status" => "OK", "time" => "2ms", "result" => []}
      ]

      assert RPC.parse_query(statements) == {:ok, [[%{"n" => 1}], []]}
    end

    test "returns the first ERR statement as an error" do
      statements = [
        %{"status" => "OK", "result" => []},
        %{"status" => "ERR", "result" => "boom"}
      ]

      assert {:error, %SurrealDB.Error{kind: :rpc, message: "boom"}} = RPC.parse_query(statements)
    end
  end

  test "action/1 normalises to lowercase atoms" do
    assert RPC.action("CREATE") == :create
    assert RPC.action("UPDATE") == :update
    assert RPC.action(:delete) == :delete
  end

  describe "engine selection" do
    test "maps schemes to engines" do
      assert Engine.for_scheme("ws") == {:ok, SurrealDB.Engine.WebSocket}
      assert Engine.for_scheme("wss") == {:ok, SurrealDB.Engine.WebSocket}
      assert Engine.for_scheme("http") == {:ok, SurrealDB.Engine.HTTP}
      assert Engine.for_scheme("https") == {:ok, SurrealDB.Engine.HTTP}
      assert {:error, _} = Engine.for_scheme("ftp")
    end
  end
end
