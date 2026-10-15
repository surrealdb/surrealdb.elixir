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

    test "keeps every statement's outcome when one fails" do
      statements = [
        %{"status" => "OK", "result" => [1]},
        %{"status" => "ERR", "result" => "boom"},
        %{"status" => "OK", "result" => [3]},
        %{"status" => "ERR", "result" => "bang"}
      ]

      assert {:error, %SurrealDB.Error{kind: :query, message: message, details: details}} =
               RPC.parse_query(statements)

      assert message == "2 of 4 statements failed; statement 2: boom"

      assert [
               {:ok, [1]},
               {:error, %SurrealDB.Error{message: "boom"}},
               {:ok, [3]},
               {:error, %SurrealDB.Error{message: "bang"}}
             ] = details.results
    end
  end

  test "action/1 normalises to lowercase atoms" do
    assert RPC.action("CREATE") == :create
    assert RPC.action("UPDATE") == :update
    assert RPC.action(:delete) == :delete
    assert RPC.action("SOMETHING_NEW") == "something_new"
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
