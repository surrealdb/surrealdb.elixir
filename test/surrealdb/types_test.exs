defmodule SurrealDB.TypesTest do
  use ExUnit.Case, async: true

  alias SurrealDB.{Datetime, Duration, RecordId, Uuid}

  describe "Datetime" do
    test "keeps nanoseconds positive before 1970" do
      {:ok, dt, _} = DateTime.from_iso8601("1969-12-31T23:59:59.250000Z")
      assert Datetime.from_datetime(dt) == %Datetime{seconds: -1, nanos: 250_000_000}
      assert dt |> Datetime.from_datetime() |> Datetime.to_datetime() == dt
    end

    test "handles whole seconds before 1970" do
      {:ok, dt, _} = DateTime.from_iso8601("1900-01-01T00:00:00Z")
      assert Datetime.from_datetime(dt) == %Datetime{seconds: -2_208_988_800, nanos: 0}
    end

    test "builds from Unix nanoseconds" do
      assert Datetime.from_unix_nanos(-1) == %Datetime{seconds: -1, nanos: 999_999_999}
      assert Datetime.from_unix_nanos(1_500_000_000) == %Datetime{seconds: 1, nanos: 500_000_000}
    end
  end

  doctest SurrealDB.FileRef
  doctest SurrealDB.RecordIdRange

  describe "RecordId" do
    test "parses and renders simple ids" do
      assert RecordId.parse!("person:tobie") == RecordId.new("person", "tobie")
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

    test "parses integer keys and quoted parts" do
      assert RecordId.parse!("person:42") == RecordId.new("person", 42)
      assert RecordId.parse!("person:-7") == RecordId.new("person", -7)
      assert RecordId.parse!("person:`42`") == RecordId.new("person", "42")
      assert RecordId.parse!("person:⟨complex id⟩") == RecordId.new("person", "complex id")
      assert RecordId.parse!("`my table`:`a\\`b`") == RecordId.new("my table", "a`b")
    end

    test "rejects strings that are not a table:key pair" do
      assert {:error, _} = RecordId.parse("person")
      assert {:error, _} = RecordId.parse("person:")
      assert {:error, _} = RecordId.parse("person:`unterminated")
      assert {:error, _} = RecordId.parse("person:a b")
      assert {:error, _} = RecordId.parse("person:[\"\\q\"]")

      # An unquoted table name must be an identifier; quoted it can be anything.
      assert {:error, _} = RecordId.parse("123:foo")
      assert {:error, _} = RecordId.parse("-users:foo")
      assert {:ok, %RecordId{table: "123", id: "foo"}} = RecordId.parse("`123`:foo")
      assert_raise ArgumentError, fn -> RecordId.parse!("nope") end
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
      assert Duration.parse("1y2w3d4h5m6s7ms8µs9ns") == %Duration{nanoseconds: ns}
      assert Duration.parse(Duration.to_string(Duration.new(365, :day))) == Duration.new(1, :year)
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

    test "parses years as 365 days" do
      assert Duration.parse("1y") == Duration.new(365, :day)
      assert Duration.parse("2y3w") == Duration.new(2 * 365 + 21, :day)
      assert Duration.new(1, :year) == Duration.new(365, :day)
    end

    test "parses microseconds written with the micro sign" do
      assert Duration.parse("5µs") == Duration.new(5, :microsecond)
    end

    test "ignores non-ASCII digits instead of raising" do
      assert Duration.parse("١y") == Duration.new(0)
      assert Duration.parse("2h١y") == Duration.new(2, :hour)
    end

    test "converts to and from parts" do
      assert Duration.from_parts(5, 250) |> Duration.to_parts() == {5, 250}
    end
  end
end
