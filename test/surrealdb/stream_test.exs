defmodule SurrealDB.StreamTest do
  use ExUnit.Case, async: true

  @moduletag :capture_log

  alias SurrealDB.{Connection, Error}

  @frames [
    %{"stream" => "begin", "version" => 1, "statements" => 2},
    %{"stream" => "rows", "index" => 0, "values" => [1, 2]},
    %{"stream" => "rows", "index" => 0, "values" => [3]},
    %{"stream" => "finished", "index" => 0, "time" => "1ms", "single" => false},
    %{"stream" => "shiny_new_frame", "index" => 1},
    %{
      "stream" => "finished",
      "index" => 1,
      "time" => "1ms",
      "single" => false,
      "error" => %{"code" => -32_006, "message" => "An error occurred: x", "kind" => "Thrown"}
    },
    %{"stream" => "end", "results" => 2, "time" => "2ms"}
  ]

  defp responder(parent, replies) do
    fn
      "__connect__", _params ->
        :ok

      method, params ->
        send(parent, {:rpc, method, params})

        case Map.fetch(replies, method) do
          {:ok, reply} -> reply
          :error -> if method == "version", do: {:ok, "surrealdb-3.3.4"}, else: {:ok, nil}
        end
    end
  end

  defp start(replies, opts \\ []) do
    base = [
      url: "ws://mock/rpc",
      engine: SurrealDB.Engine.Mock,
      responder: responder(self(), replies),
      reconnect: [delay: 10, max_delay: 50]
    ]

    start_supervised!({Connection, Keyword.merge(base, opts)})
  end

  test "stream_query sends each frame to the caller, skipping unknown kinds" do
    db = start(%{"query_stream" => {:frames, @frames}})

    assert {:ok, ref} = SurrealDB.stream_query(db, "SELECT * FROM t; THROW 'x'", %{a: 1})
    assert_receive {:rpc, "query_stream", ["SELECT * FROM t; THROW 'x'", %{"a" => 1}]}

    assert_receive {:surrealdb, :stream, ^ref, {:begin, %{statements: 2}}}
    assert_receive {:surrealdb, :stream, ^ref, {:rows, 0, [1, 2]}}
    assert_receive {:surrealdb, :stream, ^ref, {:rows, 0, [3]}}
    assert_receive {:surrealdb, :stream, ^ref, {:finished, 0, %{error: nil, single: false}}}

    assert_receive {:surrealdb, :stream, ^ref,
                    {:finished, 1, %{error: %Error{server_kind: :thrown}}}}

    assert_receive {:surrealdb, :stream, ^ref, {:end, %{results: 2, error: nil}}}
    refute_received {:surrealdb, :stream, ^ref, _}

    # The request is finished.
    assert :sys.get_state(db).pending == %{}
    assert :sys.get_state(db).streams == %{}
  end

  test "query_stream is an Elixir Stream of the frames" do
    db = start(%{"query_stream" => {:frames, @frames}})

    frames = db |> SurrealDB.query_stream("SELECT * FROM t") |> Enum.to_list()
    assert [{:begin, _}, {:rows, 0, [1, 2]}, {:rows, 0, [3]}, _, _, {:end, _}] = frames

    rows =
      db
      |> SurrealDB.query_stream("SELECT * FROM t")
      |> Enum.flat_map(fn
        {:rows, 0, rows} -> rows
        _ -> []
      end)

    assert rows == [1, 2, 3]
  end

  test "stopping a query_stream early cancels the query" do
    db = start(%{"query_stream" => {:frames, Enum.take(@frames, 3)}})

    assert [{:begin, _}, {:rows, 0, [1, 2]}] =
             db |> SurrealDB.query_stream("SELECT * FROM t") |> Enum.take(2)

    assert_receive {:rpc, "query_cancel", [id]} when is_integer(id)
    assert :sys.get_state(db).streams == %{}
    refute_received {:surrealdb, :stream, _, _}
  end

  test "a request error ends the stream with an error frame" do
    error = %{"code" => -32_601, "message" => "Method not found", "kind" => "NotFound"}
    db = start(%{"query_stream" => {:error, error}})

    assert [{:error, %Error{server_kind: :not_found}}] =
             db |> SurrealDB.query_stream("SELECT 1") |> Enum.to_list()
  end

  test "losing the connection ends the stream with a connection error" do
    db = start(%{"query_stream" => {:frames, Enum.take(@frames, 2)}})
    {:ok, ref} = SurrealDB.stream_query(db, "SELECT * FROM t")
    assert_receive {:surrealdb, :stream, ^ref, {:rows, 0, _}}

    send(db, :mock_closed)
    assert_receive {:surrealdb, :stream, ^ref, {:error, %Error{kind: :connection}}}, 1_000
  end

  test "cancel_query mutes the stream and asks the server to stop" do
    db = start(%{"query_stream" => :noreply})
    {:ok, ref} = SurrealDB.stream_query(db, "SLEEP 1s")
    assert_receive {:rpc, "query_stream", _}

    assert {:ok, _} = SurrealDB.cancel_query(db, ref)
    assert_receive {:rpc, "query_cancel", [_id]}
    assert {:ok, nil} = SurrealDB.cancel_query(db, ref)
  end

  test "gql returns per-statement results like query" do
    statements = [%{"status" => "OK", "result" => [%{"name" => "Tobie"}], "time" => "1ms"}]
    db = start(%{"gql" => {:ok, statements}})

    assert {:ok, [[%{"name" => "Tobie"}]]} = SurrealDB.gql(db, "MATCH (p:person) RETURN p")
    assert_received {:rpc, "gql", ["MATCH (p:person) RETURN p", %{}]}
  end

  test "api invokes the endpoint and returns the response" do
    response = %{"status" => 201, "headers" => %{"x" => "1"}, "body" => %{"ok" => true}}
    statements = [%{"status" => "OK", "result" => response, "time" => "1ms"}]
    db = start(%{"query" => {:ok, statements}})

    assert {:ok, %{status: 201, headers: %{"x" => "1"}, body: %{"ok" => true}}} =
             SurrealDB.api(db, "/users", method: :POST, body: %{name: "Tobie"})

    assert_received {:rpc, "query",
                     [
                       "RETURN api::invoke($path, $request)",
                       %{"path" => "/users", "request" => request}
                     ]}

    assert request == %{"method" => "post", "body" => %{"name" => "Tobie"}}
  end
end
