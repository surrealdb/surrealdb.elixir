defmodule SurrealDB.Codec.VectorsTest do
  @moduledoc """
  Runs the shared SurrealDB CBOR conformance vectors (surrealdb-protocol v0.14.0) against
  both CBOR codecs. The vectors are not committed: `scripts/fetch-protocol-vectors.sh`
  downloads them into `test/fixtures/surrealdb-protocol`, and CI runs it before the tests.
  Without them the suite is skipped.

  `cbor.yaml` runs as plain `cbor` and as `cbor.2`, except the vectors whose `formats` names
  only one of them. `cbor2.yaml` runs as `cbor.2`.
  """

  use ExUnit.Case, async: true

  alias SurrealDB.Codec.CBOR

  alias SurrealDB.{
    Bytes,
    Datetime,
    Duration,
    FileRef,
    Geometry,
    None,
    Number,
    Range,
    RecordId,
    RecordIdRange,
    Table,
    Uuid
  }

  @fixtures Path.expand("../../fixtures/surrealdb-protocol", __DIR__)
  @files [{"cbor.yaml", ["cbor", "cbor.2"]}, {"cbor2.yaml", ["cbor.2"]}]

  for {file, _formats} <- @files, do: @external_resource(Path.join(@fixtures, file))

  vectors =
    if Enum.all?(@files, fn {file, _formats} -> File.exists?(Path.join(@fixtures, file)) end) do
      for {file, default_formats} <- @files do
        for vector <- YamlElixir.read_from_file!(Path.join(@fixtures, file))["vectors"],
            format <- vector["formats"] || default_formats,
            do: {file, format, vector}
      end
    else
      []
    end

  if vectors == [] do
    @tag skip: "run scripts/fetch-protocol-vectors.sh to download the conformance vectors"
    test "conformance vectors", do: :ok
  end

  for {file, format, vector} <- List.flatten(vectors) do
    @tag vector: vector["name"]
    test "#{file} #{vector["kind"]} #{vector["name"]} as #{format}" do
      check(unquote(Macro.escape(vector)), unquote(if format == "cbor", do: :cbor, else: :cbor2))
    end
  end

  defp check(%{"kind" => "canonical"} = vector, format) do
    bytes = hex(vector["cbor"])
    expected = typed(vector["value"])

    assert {:ok, decoded} = CBOR.decode(bytes, format)
    assert_same(decoded, expected)
    assert CBOR.encode(expected, format) == bytes
  end

  defp check(%{"kind" => "decode"} = vector, format) do
    expected = typed(vector["value"])

    assert {:ok, decoded} = CBOR.decode(hex(vector["cbor"]), format)
    assert_same(decoded, expected)
    assert CBOR.encode(expected, format) == hex(vector["canonical"])
  end

  defp check(%{"kind" => "reject"} = vector, format) do
    assert {:error, %ArgumentError{}} = CBOR.decode(hex(vector["cbor"]), format)
  end

  # Strict equality: -0.0 is not 0.0, and an integer is not a float.
  defp assert_same(decoded, expected) do
    assert decoded === expected, """
    decoded value differs
      decoded:  #{inspect(decoded)}
      expected: #{inspect(expected)}
    """
  end

  defp hex(string), do: Base.decode16!(string, case: :mixed)

  # ── Typed JSON (spec/README.md) to SDK values ─────────────────────────────

  defp typed(nil), do: nil
  defp typed(value) when is_boolean(value) or is_integer(value) or is_float(value), do: value
  defp typed(value) when is_binary(value), do: value
  defp typed(list) when is_list(list), do: Enum.map(list, &typed/1)

  defp typed(map) when map_size(map) == 1 do
    case Enum.at(map, 0) do
      {"$" <> type, value} -> typed(type, value)
      _ -> object(map)
    end
  end

  defp typed(map) when is_map(map), do: object(map)

  defp typed("none", nil), do: None.none()
  defp typed("int", value), do: int(value)
  defp typed("float", value), do: float(value)
  defp typed("decimal", value), do: Decimal.new(value)
  defp typed("string", value), do: value
  defp typed("bytes", value), do: Bytes.new(hex(value))
  defp typed("uuid", value), do: Uuid.new(value)
  defp typed("datetime", value), do: datetime(value)
  defp typed("duration", [seconds, nanos]), do: Duration.from_parts(int(seconds), int(nanos))
  defp typed("array", value), do: Enum.map(value, &typed/1)
  defp typed("object", value), do: object(value)
  defp typed("set", value), do: MapSet.new(value, &typed/1)
  defp typed("table", value), do: Table.new(value)
  defp typed("file", %{"bucket" => bucket, "key" => key}), do: %FileRef{bucket: bucket, key: key}
  defp typed("geometry", value), do: geometry(value)

  defp typed("range", %{"start" => lower, "end" => upper}),
    do: %Range{begin: bound(lower), end: bound(upper)}

  defp typed("record", %{"table" => table, "key" => key}) do
    case typed(key) do
      %Range{begin: lower, end: upper} -> %RecordIdRange{table: table, begin: lower, end: upper}
      key -> %RecordId{table: table, id: key}
    end
  end

  defp typed(kind, value) when kind in ~w(i8 u8 i16 u16 i32 u32 u64),
    do: Number.new(String.to_existing_atom(kind), int(value))

  defp typed(kind, value) when kind in ~w(f16 f32),
    do: Number.new(String.to_existing_atom(kind), float(value))

  defp object(map), do: Map.new(map, fn {key, value} -> {key, typed(value)} end)

  defp int(value) when is_integer(value), do: value
  defp int(value) when is_binary(value), do: String.to_integer(value)

  defp float("NaN"), do: :nan
  defp float("Infinity"), do: :infinity
  defp float("-Infinity"), do: :neg_infinity
  # Built from its bits: before OTP 27 the compiler may fold the literal `-0.0` into `0.0`.
  defp float("-0"), do: negative_zero()
  defp float(value) when is_number(value), do: value / 1

  defp negative_zero do
    <<zero::float-64>> = <<0x8000000000000000::64>>
    zero
  end

  defp bound(nil), do: nil
  defp bound(%{"included" => value}), do: {:incl, typed(value)}
  defp bound(%{"excluded" => value}), do: {:excl, typed(value)}

  # RFC 3339 with up to nanosecond precision, which `DateTime` cannot hold.
  defp datetime(text) do
    [_, base, fraction, offset] =
      Regex.run(~r/^(.+T\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)$/, text, capture: :all)

    {:ok, datetime, _} = DateTime.from_iso8601(base <> offset)

    nanos =
      if fraction == "",
        do: 0,
        else: fraction |> String.pad_trailing(9, "0") |> String.to_integer()

    %Datetime{seconds: DateTime.to_unix(datetime), nanos: nanos}
  end

  @geometry_types %{
    "Point" => :point,
    "LineString" => :line,
    "Polygon" => :polygon,
    "MultiPoint" => :multi_point,
    "MultiLineString" => :multi_line,
    "MultiPolygon" => :multi_polygon
  }

  defp geometry(%{"type" => "GeometryCollection", "geometries" => geometries}),
    do: %Geometry{type: :collection, coordinates: Enum.map(geometries, &geometry/1)}

  defp geometry(%{"type" => type, "coordinates" => coordinates}),
    do: %Geometry{type: Map.fetch!(@geometry_types, type), coordinates: floats(coordinates)}

  defp floats(list) when is_list(list), do: Enum.map(list, &floats/1)
  defp floats(number), do: float(number)
end
