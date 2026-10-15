defmodule SurrealDB.Codec.CBORTest do
  use ExUnit.Case, async: true

  alias SurrealDB.Codec.CBOR

  alias SurrealDB.{
    Bytes,
    Datetime,
    Duration,
    FileRef,
    Future,
    Geometry,
    None,
    Range,
    RecordId,
    RecordIdRange,
    Table,
    Uuid
  }

  defp roundtrip(term) do
    {:ok, decoded} = term |> CBOR.encode() |> CBOR.decode()
    decoded
  end

  describe "scalars and containers" do
    test "round-trips primitive values" do
      for value <- [0, 1, 23, 24, 255, 256, 65_536, -1, -1000, 3.14, true, false, nil, ""] do
        assert roundtrip(value) == value
      end
    end

    test "round-trips strings, lists, and maps" do
      assert roundtrip("hello") == "hello"
      assert roundtrip([1, "two", [3]]) == [1, "two", [3]]
      assert roundtrip(%{"a" => 1, "b" => [true, nil]}) == %{"a" => 1, "b" => [true, nil]}
    end

    test "encodes atom map keys as strings" do
      assert roundtrip(%{name: "tobie"}) == %{"name" => "tobie"}
    end
  end

  describe "SurrealDB value types" do
    test "RecordId with string and integer ids" do
      assert roundtrip(RecordId.new("person", "tobie")) == RecordId.new("person", "tobie")
      assert roundtrip(RecordId.new("person", 42)) == RecordId.new("person", 42)
    end

    test "StringRecordId encodes as a record id tag holding the text" do
      assert CBOR.encode(SurrealDB.StringRecordId.new("a:b")) == <<0xC8, 0x63, "a:b">>
    end

    test "Table" do
      assert roundtrip(Table.new("person")) == Table.new("person")
    end

    test "Uuid round-trips through binary form" do
      uuid = Uuid.new("0190d6f2-8b1e-7000-8000-000000000abc")
      assert roundtrip(uuid) == uuid
    end

    test "Duration preserves nanoseconds" do
      duration = Duration.new(90, :minute)
      assert roundtrip(duration) == duration
      assert roundtrip(Duration.from_parts(5, 250)) == Duration.from_parts(5, 250)
    end

    test "Datetime preserves seconds and nanos" do
      datetime = Datetime.from_unix(1_700_000_000, 123_456_789)
      assert roundtrip(datetime) == datetime
    end

    test "native DateTime encodes and decodes to a Datetime" do
      {:ok, dt, _} = DateTime.from_iso8601("2024-01-02T03:04:05Z")
      assert %Datetime{} = decoded = roundtrip(dt)
      assert decoded.seconds == DateTime.to_unix(dt)
    end

    test "Decimal" do
      decimal = Decimal.new("3.14159265358979")
      assert roundtrip(decimal) == decimal
    end

    test "Geometry point and collection" do
      point = Geometry.point(-0.118, 51.509)
      assert roundtrip(point) == point

      collection = Geometry.collection([Geometry.point(0, 0), Geometry.point(1, 1)])
      assert roundtrip(collection) == collection
    end

    test "Range with inclusive and exclusive bounds" do
      range = Range.new({:incl, 1}, {:excl, 10})
      assert roundtrip(range) == range
      assert roundtrip(Range.new(nil, {:incl, 5})) == Range.new(nil, {:incl, 5})
    end

    test "RecordIdRange is a record id whose key is a range" do
      range = RecordIdRange.new("person", {:incl, 1}, {:excl, 100})
      assert roundtrip(range) == range
      assert roundtrip(RecordIdRange.new("person", nil, {:incl, "m"})).end == {:incl, "m"}

      # tag 8, [ "person", tag 49 [ tag 50 1, tag 51 100 ] ]
      assert CBOR.encode(range) ==
               <<0xC8, 0x82, 0x66, "person", 0xD8, 49, 0x82, 0xD8, 50, 0x01, 0xD8, 51, 0x18, 100>>
    end

    test "FileRef is tag 55 with the bucket and key" do
      file = FileRef.new("avatars", "tobie.png")
      assert file.key == "/tobie.png"
      assert roundtrip(file) == file
      assert <<0xD8, 55, 0x82, _::binary>> = CBOR.encode(file)
    end

    test "a MapSet is a set, tag 56 with its elements" do
      set = MapSet.new([1, "a", RecordId.new("t", 1)])
      assert roundtrip(set) == set
      assert <<0xD8, 56, 0x83, _::binary>> = CBOR.encode(set)
      assert roundtrip(MapSet.new()) == MapSet.new()
    end

    test "Bytes are distinguished from text" do
      bytes = Bytes.new(<<0, 1, 2, 255>>)
      assert roundtrip(bytes) == bytes
      refute match?(%Bytes{}, roundtrip("plain string"))
    end

    test "None and Future" do
      assert roundtrip(None.none()) == None.none()
      assert %Future{} = roundtrip(Future.new())
    end
  end

  describe "decode errors" do
    test "returns an error tuple for truncated input" do
      assert {:error, _} = CBOR.decode(<<0x82, 0x01>>)
    end
  end
end
