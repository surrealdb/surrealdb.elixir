defmodule SurrealDB.RestTest do
  use ExUnit.Case, async: true

  alias SurrealDB.{Error, Rest}

  setup do
    bypass = Bypass.open()

    session = %{
      uri: URI.parse("ws://localhost:#{bypass.port}/rpc"),
      namespace: "n",
      database: "d"
    }

    {:ok, bypass: bypass, session: session}
  end

  test "export with no options GETs everything", %{bypass: bypass, session: session} do
    Bypass.expect_once(bypass, "GET", "/export", fn conn ->
      assert Plug.Conn.get_req_header(conn, "surreal-ns") == ["n"]
      Plug.Conn.resp(conn, 200, "OPTION IMPORT;")
    end)

    assert {:ok, "OPTION IMPORT;"} = Rest.export(session, [])
  end

  test "export options are POSTed as the config", %{bypass: bypass, session: session} do
    Bypass.expect_once(bypass, "POST", "/export", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      assert Jason.decode!(body) == %{
               "tables" => %{"exclude" => ["secret"]},
               "records" => false,
               "users" => false
             }

      Plug.Conn.resp(conn, 200, "OPTION IMPORT;")
    end)

    assert {:ok, _} =
             Rest.export(session,
               tables: {:exclude, ["secret"]},
               records: false,
               users: false,
               timeout: 5_000
             )
  end

  test "an invalid export option raises", %{session: session} do
    assert_raise ArgumentError, fn -> Rest.export(session, records: "yes") end
  end

  test "an import reports each failed statement", %{bypass: bypass, session: session} do
    failures = [
      %{"status" => "ERR", "kind" => "Thrown", "result" => "An error occurred: bad"},
      %{
        "status" => "ERR",
        "kind" => "AlreadyExists",
        "result" => "Database record `a:5` already exists",
        "details" => %{"kind" => "Record", "details" => %{"id" => "a:5"}}
      }
    ]

    Bypass.expect_once(bypass, "POST", "/import", fn conn ->
      Plug.Conn.resp(conn, 422, Jason.encode!(failures))
    end)

    assert {:error, %Error{kind: :query, code: 422, message: message, details: details}} =
             Rest.import(session, "OPTION IMPORT; ...", [])

    assert message == "import: 2 statements failed; first: An error occurred: bad"

    assert [%Error{server_kind: :thrown}, %Error{server_kind: :already_exists} = exists] =
             details.failures

    assert Error.detail_fields(exists) == %{"id" => "a:5"}
  end

  test "an import the server rejects says why", %{bypass: bypass, session: session} do
    Bypass.expect_once(bypass, "POST", "/import", fn conn ->
      body = %{"code" => 400, "information" => "Import requires `OPTION IMPORT;`"}
      Plug.Conn.resp(conn, 400, Jason.encode!(body))
    end)

    assert {:error,
            %Error{kind: :http, code: 400, message: "import failed: Import requires" <> _}} =
             Rest.import(session, "CREATE a", [])
  end

  test "an unauthorised import is an auth error", %{bypass: bypass, session: session} do
    Bypass.expect_once(bypass, "POST", "/import", fn conn ->
      Plug.Conn.resp(conn, 401, "There was a problem with authentication")
    end)

    assert {:error, %Error{kind: :auth, code: 401}} = Rest.import(session, "OPTION IMPORT;", [])
  end
end
