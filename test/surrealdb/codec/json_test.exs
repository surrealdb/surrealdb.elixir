defmodule SurrealDB.Codec.JSONTest do
  use ExUnit.Case, async: true

  alias SurrealDB.Codec.JSON
  alias SurrealDB.{Bytes, Geometry, None, RecordId, Table, Uuid}

  defp encode(term), do: term |> JSON.encode() |> IO.iodata_to_binary() |> Jason.decode!()

  test "projects value types onto JSON friendly forms" do
    assert encode(RecordId.new("person", "tobie")) == "person:tobie"
    assert encode(Table.new("person")) == "person"

    assert encode(Uuid.new("0190d6f2-8b1e-7000-8000-000000000abc")) ==
             "0190d6f2-8b1e-7000-8000-000000000abc"

    assert encode(None.none()) == nil
    assert encode(Decimal.new("1.5")) == "1.5"
    assert encode(Bytes.new(<<1, 2, 3>>)) == Base.encode64(<<1, 2, 3>>)
  end

  test "geometry becomes GeoJSON" do
    assert encode(Geometry.point(1.0, 2.0)) == %{"type" => "Point", "coordinates" => [1.0, 2.0]}
  end

  test "nested maps and lists are projected recursively" do
    term = %{user: RecordId.new("person", 1), tags: [Table.new("a")]}
    assert encode(term) == %{"user" => "person:1", "tags" => ["a"]}
  end

  test "decodes JSON to maps with string keys" do
    assert {:ok, %{"a" => 1}} = JSON.decode(~s({"a":1}))
  end
end
