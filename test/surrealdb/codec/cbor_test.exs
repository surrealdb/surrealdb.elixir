defmodule SurrealDB.Codec.CBORTest do
  use ExUnit.Case, async: true

  alias SurrealDB.Codec.CBOR

  alias SurrealDB.{
    Bytes,
    Datetime,
    Duration,
    Future,
    Geometry,
    None,
    Range,
    RecordId,
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

  describe "what the encoder sends" do
    # Collects every tag in an encoded value (definite lengths only, as the encoder writes).
    defp tags(binary) do
      {tags, ""} = walk(binary, [])
      Enum.sort(Enum.uniq(tags))
    end

    defp walk(<<major::3, info::5, rest::binary>>, tags) do
      {arg, rest} = argument(info, rest)

      case major do
        n when n in [0, 1, 7] -> {tags, rest}
        n when n in [2, 3] -> {tags, binary_part(rest, arg, byte_size(rest) - arg)}
        4 -> walk_items(rest, arg, tags)
        5 -> walk_items(rest, arg * 2, tags)
        6 -> walk(rest, [arg | tags])
      end
    end

    defp walk_items(rest, 0, tags), do: {tags, rest}

    defp walk_items(rest, count, tags) do
      {tags, rest} = walk(rest, tags)
      walk_items(rest, count - 1, tags)
    end

    defp argument(info, rest) when info < 24, do: {info, rest}
    defp argument(24, <<n::8, rest::binary>>), do: {n, rest}
    defp argument(25, <<n::16, rest::binary>>), do: {n, rest}
    defp argument(26, <<n::32, rest::binary>>), do: {n, rest}
    defp argument(27, <<n::64, rest::binary>>), do: {n, rest}

    test "datetimes, uuids and durations use the binary tags, never the text tags 0, 9 and 13" do
      uuid = Uuid.new("0190d6f2-8b1e-7000-8000-000000000000")

      value = %{
        "at" => ~U[2024-01-02 03:04:05.123456Z],
        "before" => ~U[1969-12-31 23:59:59.250000Z],
        "raw" => Datetime.from_unix(1_700_000_000, 5),
        "id" => uuid,
        "for" => Duration.new(90, :minute),
        "rec" => RecordId.new("t", uuid),
        "list" => [uuid, Duration.new(1, :second), Datetime.from_unix(0)]
      }

      tags = value |> CBOR.encode() |> tags()

      assert tags == [8, 12, 14, 37]
      refute Enum.any?(tags, &(&1 in [0, 9, 13]))
    end

    test "None is tag 6 holding null, nothing else" do
      assert CBOR.encode(None.none()) == <<0xC6, 0xF6>>
    end

    test "integers up to the 64-bit signed limits encode" do
      for value <- [9_223_372_036_854_775_807, -9_223_372_036_854_775_808] do
        assert roundtrip(value) == value
      end
    end

    test "integers outside the 64-bit signed range are refused instead of truncated" do
      for value <- [
            9_223_372_036_854_775_808,
            -9_223_372_036_854_775_809,
            18_446_744_073_709_551_616,
            -18_446_744_073_709_551_617
          ] do
        assert_raise ArgumentError, ~r/64-bit signed range/, fn -> CBOR.encode(value) end

        assert_raise ArgumentError, ~r/64-bit signed range/, fn ->
          CBOR.encode([%{"n" => value}])
        end
      end

      big = 18_446_744_073_709_551_616
      assert roundtrip(Decimal.new(big)) == Decimal.new(big)
    end

    test "map keys that collide once atoms are strings are refused" do
      assert_raise ArgumentError, ~r/duplicate map key "a"/, fn ->
        CBOR.encode(%{:a => 1, "a" => 2})
      end

      assert roundtrip(%{:a => 1, "b" => 2, 3 => 4}) == %{"a" => 1, "b" => 2, 3 => 4}
    end
  end

  describe "decode errors" do
    test "returns an error tuple for truncated input" do
      assert {:error, _} = CBOR.decode(<<0x82, 0x01>>)
    end
  end
end
