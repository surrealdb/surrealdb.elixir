defmodule SurrealDB.ConnectionTest do
  use ExUnit.Case, async: true

  alias SurrealDB.{Codec, RecordId, Table, Uuid}

  @live_uuid "0190d6f2-8b1e-7000-8000-000000000001"

  defp responder do
    fn
      "use", _params -> {:ok, nil}
      "signin", _params -> {:ok, "jwt.token"}
      "create", [thing, data] -> {:ok, [Map.put(data, "thing", inspect(thing))]}
      "select", [thing] -> {:ok, %{"selected" => inspect(thing)}}
      "query", _params -> {:ok, [%{"status" => "OK", "time" => "1ms", "result" => [1, 2, 3]}]}
      "query_err", _params -> {:ok, [%{"status" => "ERR", "result" => "bad query"}]}
      "live", [_thing, _diff] -> {:ok, Uuid.new(@live_uuid)}
      "kill", _params -> {:ok, nil}
      "boom", _params -> {:error, %{"code" => -32_000, "message" => "explosion"}}
      method, params -> {:ok, %{"method" => method, "params" => params}}
    end
  end

  defp start(opts \\ []) do
    base = [url: "ws://mock/rpc", engine: SurrealDB.Engine.Mock, responder: responder()]
    start_supervised!({SurrealDB.Connection, Keyword.merge(base, opts)})
  end

  test "connects and authenticates on setup" do
    db =
      start(
        namespace: "test",
        database: "app",
        auth: %SurrealDB.Auth.Root{user: "root", pass: "root"}
      )

    info = SurrealDB.info_session(db)

    assert info.namespace == "test"
    assert info.database == "app"
    assert info.authenticated
    assert info.supports_live?
  end

  test "create normalises a bare table name to a Table" do
    db = start()
    {:ok, [record]} = SurrealDB.create(db, "person", %{"name" => "Tobie"})
    assert record["thing"] == inspect(Table.new("person"))
  end

  test "select normalises a colon string to a RecordId" do
    db = start()
    {:ok, result} = SurrealDB.select(db, "person:tobie")
    assert result["selected"] == inspect(RecordId.parse("person:tobie"))
  end

  test "query returns one result per statement" do
    db = start()
    assert SurrealDB.query(db, "SELECT 1") == {:ok, [[1, 2, 3]]}
  end

  test "query_raw returns the raw statement objects" do
    db = start()

    assert {:ok, [%{"status" => "OK", "result" => [1, 2, 3]}]} =
             SurrealDB.query_raw(db, "SELECT 1")
  end

  test "rpc errors are surfaced as SurrealDB.Error" do
    db = start()

    assert {:error, %SurrealDB.Error{kind: :rpc, message: "explosion", code: -32_000}} =
             SurrealDB.Connection.call(db, "boom", [])
  end

  test "let and unset update parameters" do
    db = start()
    assert {:ok, _} = SurrealDB.let(db, "x", 1)
    assert {:ok, _} = SurrealDB.unset(db, "x")
  end

  describe "live queries" do
    test "delivers notifications to the subscriber and stops on kill" do
      db = start()
      {:ok, live_id} = SurrealDB.live(db, "person")
      assert %Uuid{value: @live_uuid} = live_id

      notification = %{
        "id" => Uuid.new(@live_uuid),
        "action" => "CREATE",
        "result" => %{"id" => "person:new", "name" => "New"}
      }

      send(db, {:mock_message, Codec.CBOR.encode(notification)})

      assert_receive {:surrealdb, :live, _id, %{action: :create, result: result}}, 1_000
      assert result == %{"id" => "person:new", "name" => "New"}

      assert {:ok, _} = SurrealDB.kill(db, live_id)
    end

    test "routes notifications to a custom subscriber pid" do
      db = start()
      parent = self()
      subscriber = spawn(fn -> relay(parent) end)
      {:ok, _live_id} = SurrealDB.live(db, "person", to: subscriber)

      notification = %{"id" => Uuid.new(@live_uuid), "action" => "UPDATE", "result" => %{}}
      send(db, {:mock_message, Codec.CBOR.encode(notification)})

      assert_receive {:relayed, {:surrealdb, :live, _id, %{action: :update}}}, 1_000
    end
  end

  defp relay(parent) do
    receive do
      message -> send(parent, {:relayed, message})
    end
  end
end
