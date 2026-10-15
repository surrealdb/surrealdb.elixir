defmodule SurrealDB.TypesTest do
  use ExUnit.Case, async: true

  alias SurrealDB.{Duration, RecordId, Uuid}

  describe "RecordId" do
    test "parses and renders simple ids" do
      assert RecordId.parse("person:tobie") == RecordId.new("person", "tobie")
      assert RecordId.to_string(RecordId.new("person", "tobie")) == "person:tobie"
    end

    test "escapes non-simple identifiers with backticks" do
      assert RecordId.to_string(RecordId.new("person", "complex id")) == "person:`complex id`"
      assert RecordId.to_string(RecordId.new("person", "a`b\\c")) == "person:`a\\`b\\\\c`"
    end

    test "quotes all-digit string keys so they do not read as numbers" do
      assert RecordId.to_string(RecordId.new("person", "123")) == "person:`123`"
      assert RecordId.to_string(RecordId.new("person", 123)) == "person:123"
      assert RecordId.to_string(RecordId.new("person", "")) == "person:``"
    end

    test "quotes table names that are keywords or start with a digit" do
      assert RecordId.to_string(RecordId.new("select", "x")) == "`select`:x"
      assert RecordId.to_string(RecordId.new("1a", "x")) == "`1a`:x"
      assert RecordId.to_string(RecordId.new("person", "select")) == "person:select"
    end

    test "renders array, object, and uuid keys as SurrealQL, not inspect output" do
      assert RecordId.to_string(RecordId.new("temp", ["london", 2024])) == "temp:['london', 2024]"

      assert RecordId.to_string(RecordId.new("temp", %{"city" => "london", "at" => 1.5})) ==
               "temp:{ at: 1.5f, city: 'london' }"

      uuid = Uuid.new("0190d6f2-8b1e-7000-8000-000000000000")

      assert RecordId.to_string(RecordId.new("temp", uuid)) ==
               "temp:u'0190d6f2-8b1e-7000-8000-000000000000'"
    end

    test "String.Chars protocol" do
      assert to_string(RecordId.new("person", 1)) == "person:1"
    end
  end

  describe "Duration.to_string/1" do
    test "formats like the server" do
      assert Duration.to_string(Duration.new(0)) == "0ns"
      assert Duration.to_string(Duration.new(90, :minute)) == "1h30m"

      ns =
        Duration.new(365, :day).nanoseconds + Duration.new(2, :week).nanoseconds +
          Duration.new(3, :day).nanoseconds + 4 * 3_600_000_000_000 + 5 * 60_000_000_000 +
          6_007_008_009

      assert Duration.to_string(%Duration{nanoseconds: ns}) == "1y2w3d4h5m6s7ms8µs9ns"
    end
  end

  describe "Uuid" do
    test "binary round-trip" do
      uuid = Uuid.new("0190D6F2-8B1E-7000-8000-000000000ABC")
      assert uuid.value == "0190d6f2-8b1e-7000-8000-000000000abc"
      assert uuid |> Uuid.to_binary() |> Uuid.from_binary() == uuid
    end
  end

  describe "Duration" do
    test "parses compound duration strings" do
      assert Duration.parse("1h30m") == Duration.new(90, :minute)
      assert Duration.parse("500ms").nanoseconds == 500_000_000
      assert Duration.parse("2w") == Duration.new(2, :week)
    end

    test "converts to and from parts" do
      assert Duration.from_parts(5, 250) |> Duration.to_parts() == {5, 250}
    end
  end
end
