defmodule SurrealDB.Codec.JSON do
  @moduledoc """
  JSON protocol codec, backed by `Jason`.

  JSON cannot represent the full set of SurrealDB types, so the value types are projected
  onto their closest JSON form on the way out (for example a `RecordId` becomes its
  `table:id` string and a `Geometry` becomes a GeoJSON object). Decoded values come back as
  plain maps, lists, and scalars with string keys.

  Prefer `SurrealDB.Codec.CBOR` when you need full type fidelity. JSON is useful for simple
  payloads or when interoperating with tooling that expects JSON on the wire.
  """

  @behaviour SurrealDB.Codec

  alias SurrealDB.{Bytes, Datetime, Duration, Geometry, None, Range, RecordId, Table, Uuid}

  @impl true
  def content_type, do: "application/json"

  @impl true
  def subprotocol, do: "json"

  @impl true
  def encode(term), do: Jason.encode_to_iodata!(prepare(term))

  @impl true
  def decode(binary) when is_binary(binary), do: Jason.decode(binary)

  defp prepare(%None{}), do: nil
  defp prepare(%Table{name: name}), do: name
  defp prepare(%RecordId{} = record), do: RecordId.to_string(record)
  defp prepare(%Uuid{value: value}), do: value
  defp prepare(%Decimal{} = decimal), do: Decimal.to_string(decimal, :normal)

  defp prepare(%Datetime{} = datetime),
    do: datetime |> Datetime.to_datetime() |> DateTime.to_iso8601()

  defp prepare(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp prepare(%Duration{nanoseconds: nanoseconds}), do: "#{nanoseconds}ns"
  defp prepare(%Bytes{data: data}), do: Base.encode64(data)

  defp prepare(%Geometry{type: :collection, coordinates: geometries}) do
    %{"type" => "GeometryCollection", "geometries" => Enum.map(geometries, &prepare/1)}
  end

  defp prepare(%Geometry{type: type, coordinates: coordinates}) do
    %{"type" => geojson_type(type), "coordinates" => coordinates}
  end

  defp prepare(%Range{begin: begin_bound, end: end_bound}) do
    %{"begin" => prepare_bound(begin_bound), "end" => prepare_bound(end_bound)}
  end

  defp prepare(list) when is_list(list), do: Enum.map(list, &prepare/1)

  defp prepare(%_struct{} = struct), do: struct |> Map.from_struct() |> prepare()

  defp prepare(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {key, prepare(value)} end)
  end

  defp prepare(other), do: other

  defp prepare_bound(nil), do: nil
  defp prepare_bound({:incl, value}), do: %{"inclusive" => true, "value" => prepare(value)}
  defp prepare_bound({:excl, value}), do: %{"inclusive" => false, "value" => prepare(value)}

  defp geojson_type(:point), do: "Point"
  defp geojson_type(:line), do: "LineString"
  defp geojson_type(:polygon), do: "Polygon"
  defp geojson_type(:multi_point), do: "MultiPoint"
  defp geojson_type(:multi_line), do: "MultiLineString"
  defp geojson_type(:multi_polygon), do: "MultiPolygon"
end
