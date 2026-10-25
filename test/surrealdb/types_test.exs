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
      assert {:error, _} = RecordId.parse("temp:['london', 2024]")

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
