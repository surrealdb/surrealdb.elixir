defmodule SurrealDB.ErrorTest do
  use ExUnit.Case, async: true

  alias SurrealDB.Error

  test "from_rpc keeps the server's kind, details, and cause" do
    error =
      Error.from_rpc(%{
        "code" => -32_002,
        "message" => "Token expired",
        "kind" => "NotAllowed",
        "details" => %{"kind" => "Auth", "details" => %{"kind" => "TokenExpired"}},
        "cause" => %{"code" => -32_000, "message" => "inner", "kind" => "Internal"}
      })

    assert %Error{kind: :rpc, code: -32_002, server_kind: :not_allowed} = error
    assert Error.detail_kinds(error) == ["Auth", "TokenExpired"]
    assert Error.token_expired?(error)
    refute Error.session_expired?(error)
    assert %Error{message: "inner", server_kind: :internal} = error.cause
  end

  test "NONE and empty details or cause are treated as absent" do
    error =
      Error.from_rpc(%{
        "code" => -32_000,
        "message" => "x",
        "kind" => "Internal",
        "details" => %{},
        "cause" => SurrealDB.None.none()
      })

    assert error.details == nil
    assert error.cause == nil
    assert Error.detail_kinds(error) == []
  end

  test "derives the kind from the code when the server sends none" do
    assert Error.from_rpc(%{"code" => -32_601, "message" => "m"}).server_kind == :not_found
    assert Error.from_rpc(%{"code" => -32_009, "message" => "m"}).server_kind == :query
    assert Error.from_rpc(%{"code" => 1, "message" => "m"}).server_kind == :internal
    assert Error.from_rpc(%{"message" => "m"}).server_kind == nil
  end

  test "unknown kinds are treated as internal" do
    assert Error.from_rpc(%{"code" => 1, "message" => "m", "kind" => "Brand"}).server_kind ==
             :internal
  end

  test "detail_fields returns the innermost fields" do
    error =
      Error.from_rpc(%{
        "code" => -32_000,
        "message" => "The table 'person' does not exist",
        "kind" => "NotFound",
        "details" => %{"kind" => "Table", "details" => %{"name" => "person"}}
      })

    assert Error.detail_kinds(error) == ["Table"]
    assert Error.detail_fields(error) == %{"name" => "person"}
  end

  test "from_statement reads a failed query statement" do
    error =
      Error.from_statement(%{
        "status" => "ERR",
        "result" => "The namespace 't' does not exist",
        "kind" => "NotFound",
        "details" => %{"kind" => "Namespace", "details" => %{"name" => "t"}}
      })

    assert %Error{kind: :rpc, server_kind: :not_found, code: nil} = error
    assert Error.detail_kinds(error) == ["Namespace"]
  end

  test "from_statement drops a repeated kind wrapper" do
    error =
      Error.from_statement(%{
        "result" => "conflict",
        "kind" => "Query",
        "details" => %{"kind" => "Query", "details" => %{"kind" => "TransactionConflict"}}
      })

    assert Error.detail_kinds(error) == ["TransactionConflict"]
    assert Error.transaction_conflict?(error)
  end

  test "the exception message names the kinds and the code" do
    error = Error.from_rpc(%{"code" => -32_000, "message" => "boom", "kind" => "Internal"})
    assert Exception.message(error) == "[rpc:internal] (-32000) boom"
    assert Exception.message(Error.new(:timeout, "late")) == "[timeout] late"
  end
end
