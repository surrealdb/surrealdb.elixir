defmodule SurrealDB.SurrealQLTest do
  use ExUnit.Case, async: true

  alias SurrealDB.{Bytes, Datetime, Geometry, None, Range, RecordId, SurrealQL}

  doctest SurrealDB.SurrealQL
  doctest SurrealDB.RecordId

  test "renders scalars as the server does" do
    assert SurrealQL.to_surql(nil) == "NULL"
    assert SurrealQL.to_surql(None.none()) == "NONE"
    assert SurrealQL.to_surql(1.0e10) == "10000000000f"
    assert SurrealQL.to_surql(1.0e-7) == "0.0000001f"
    assert SurrealQL.to_surql(1.0) == "1f"
    assert SurrealQL.to_surql(-2.5) == "-2.5f"
    assert SurrealQL.to_surql(Decimal.new("1.50")) == "1.5dec"
    assert SurrealQL.to_surql(Bytes.new(<<1, 2, 255>>)) == ~s(b"0102FF")
    assert SurrealQL.to_surql(Geometry.point(1.5, 2)) == "(1.5, 2)"
    assert SurrealQL.to_surql(%{}) == "{  }"
    assert SurrealQL.to_surql([]) == "[]"
  end

  test "quotes strings with single quotes unless they contain one" do
    assert SurrealQL.to_surql("plain") == "'plain'"
    assert SurrealQL.to_surql("a'b") == ~s("a'b")
    assert SurrealQL.to_surql("tab\tx\\") == "'tab\\tx\\\\'"
  end

  test "renders datetimes with full precision" do
    assert SurrealQL.to_surql(Datetime.from_unix(1_577_836_800, 123_456_789)) ==
             "d'2020-01-01T00:00:00.123456789Z'"

    assert SurrealQL.to_surql(Datetime.from_unix(-1, 250_000_000)) ==
             "d'1969-12-31T23:59:59.25Z'"
  end

  test "renders ranges" do
    assert SurrealQL.to_surql(Range.new({:incl, 1}, {:excl, 5})) == "1..5"
    assert SurrealQL.to_surql(Range.new({:excl, 1}, {:incl, 5})) == "1>..=5"
    assert SurrealQL.to_surql(Range.new(nil, nil)) == ".."
    assert SurrealQL.to_surql(RecordId.new("t", Range.new({:incl, "a"}, nil))) == "t:a.."
  end
end
