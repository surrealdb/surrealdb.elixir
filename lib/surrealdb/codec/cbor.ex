defmodule SurrealDB.Codec.CBOR do
  @moduledoc """
  CBOR protocol codec with SurrealDB custom tags.

  This is the default protocol. It preserves the full set of SurrealDB value types by
  mapping each one to its custom CBOR tag:

  | Tag | Type            | Tag | Type                 |
  | --- | --------------- | --- | -------------------- |
  | 6   | `None`          | 49  | `Range`              |
  | 7   | `Table`         | 50  | Range bound included |
  | 8   | `RecordId`      | 51  | Range bound excluded |
  | 9   | `Uuid` (string) | 88  | Geometry point       |
  | 10  | `Decimal`       | 89  | Geometry line        |
  | 12  | `Datetime`      | 90  | Geometry polygon     |
  | 13  | `Duration` str  | 91  | Geometry multi-point |
  | 14  | `Duration` pair | 92  | Geometry multi-line  |
  | 15  | `Future`        | 93  | Geometry multi-poly  |
  | 37  | `Uuid` (binary) | 94  | Geometry collection  |

  The implementation is a focused CBOR encoder and decoder covering the major types the
  protocol uses, rather than a general purpose CBOR library, so the custom tags are handled
  natively in both directions.
  """

  @behaviour SurrealDB.Codec

  import Bitwise

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

  @impl true
  def content_type, do: "application/cbor"

  @impl true
  def subprotocol, do: "cbor"

  @impl true
  def encode(term), do: IO.iodata_to_binary(enc(term))

  @impl true
  def decode(binary) when is_binary(binary) do
    {value, _rest} = decode_item(binary)
    {:ok, value}
  rescue
    error -> {:error, error}
  catch
    :throw, reason -> {:error, reason}
  end

  # ── Encoding ──────────────────────────────────────────────────────────────

  defp enc(false), do: <<0xF4>>
  defp enc(true), do: <<0xF5>>
  defp enc(nil), do: <<0xF6>>

  defp enc(%None{}), do: [head(6, 6), enc(nil)]
  defp enc(%Table{name: name}), do: [head(6, 7), enc(name)]
  defp enc(%RecordId{table: table, id: id}), do: [head(6, 8), enc([table, id])]

  defp enc(%Uuid{} = uuid) do
    [head(6, 37), enc(Bytes.new(Uuid.to_binary(uuid)))]
  end

  defp enc(%Decimal{} = decimal), do: [head(6, 10), enc(Decimal.to_string(decimal, :normal))]

  defp enc(%Datetime{seconds: seconds, nanos: nanos}) do
    [head(6, 12), enc([seconds, nanos])]
  end

  defp enc(%DateTime{} = datetime), do: enc(Datetime.from_datetime(datetime))

  defp enc(%Duration{} = duration) do
    {seconds, nanos} = Duration.to_parts(duration)
    [head(6, 14), enc([seconds, nanos])]
  end

  defp enc(%Future{}), do: [head(6, 15), enc(nil)]

  defp enc(%Range{begin: begin_bound, end: end_bound}) do
    array = [head(4, 2), enc_bound(begin_bound), enc_bound(end_bound)]
    [head(6, 49), array]
  end

  defp enc(%Geometry{type: :collection, coordinates: geometries}) do
    array = [head(4, length(geometries)) | Enum.map(geometries, &enc/1)]
    [head(6, 94), array]
  end

  defp enc(%Geometry{type: type, coordinates: coordinates}) do
    [head(6, geometry_tag(type)), enc(coordinates)]
  end

  defp enc(%Bytes{data: data}), do: [head(2, byte_size(data)), data]

  defp enc(integer) when is_integer(integer) and integer >= 0, do: head(0, integer)
  defp enc(integer) when is_integer(integer), do: head(1, -1 - integer)
  defp enc(float) when is_float(float), do: <<0xFB, float::float-64>>

  defp enc(atom) when is_atom(atom), do: enc(Atom.to_string(atom))

  defp enc(binary) when is_binary(binary), do: [head(3, byte_size(binary)), binary]

  defp enc(list) when is_list(list) do
    [head(4, length(list)) | Enum.map(list, &enc/1)]
  end

  defp enc(map) when is_map(map) do
    pairs = Enum.map(map, fn {key, value} -> [enc(map_key(key)), enc(value)] end)
    [head(5, map_size(map)) | pairs]
  end

  defp map_key(key) when is_atom(key), do: Atom.to_string(key)
  defp map_key(key), do: key

  defp enc_bound(nil), do: enc(nil)
  defp enc_bound({:incl, value}), do: [head(6, 50), enc(value)]
  defp enc_bound({:excl, value}), do: [head(6, 51), enc(value)]

  defp geometry_tag(:point), do: 88
  defp geometry_tag(:line), do: 89
  defp geometry_tag(:polygon), do: 90
  defp geometry_tag(:multi_point), do: 91
  defp geometry_tag(:multi_line), do: 92
  defp geometry_tag(:multi_polygon), do: 93

  defp head(major, n) when n < 0x18, do: <<major <<< 5 ||| n>>
  defp head(major, n) when n < 0x100, do: <<major <<< 5 ||| 0x18, n::8>>
  defp head(major, n) when n < 0x10000, do: <<major <<< 5 ||| 0x19, n::16>>
  defp head(major, n) when n < 0x100000000, do: <<major <<< 5 ||| 0x1A, n::32>>
  defp head(major, n), do: <<major <<< 5 ||| 0x1B, n::64>>

  # ── Decoding ──────────────────────────────────────────────────────────────

  # Major type 7: simple values and floats, matched as exact bytes first.
  defp decode_item(<<0xF4, rest::binary>>), do: {false, rest}
  defp decode_item(<<0xF5, rest::binary>>), do: {true, rest}
  defp decode_item(<<0xF6, rest::binary>>), do: {nil, rest}
  defp decode_item(<<0xF7, rest::binary>>), do: {nil, rest}
  defp decode_item(<<0xF9, half::16, rest::binary>>), do: {half_to_float(half), rest}
  defp decode_item(<<0xFA, float::float-32, rest::binary>>), do: {float, rest}
  defp decode_item(<<0xFB, float::float-64, rest::binary>>), do: {float, rest}
  defp decode_item(<<0xF8, _simple, rest::binary>>), do: {nil, rest}

  defp decode_item(<<byte, _::binary>> = bin) do
    major = byte >>> 5
    info = byte &&& 0x1F
    <<_, rest::binary>> = bin
    {arg, rest} = read_arg(info, rest)
    build(major, arg, rest)
  end

  defp read_arg(info, rest) when info < 24, do: {info, rest}
  defp read_arg(24, <<arg, rest::binary>>), do: {arg, rest}
  defp read_arg(25, <<arg::16, rest::binary>>), do: {arg, rest}
  defp read_arg(26, <<arg::32, rest::binary>>), do: {arg, rest}
  defp read_arg(27, <<arg::64, rest::binary>>), do: {arg, rest}
  defp read_arg(31, rest), do: {:indefinite, rest}

  defp build(0, arg, rest), do: {arg, rest}
  defp build(1, arg, rest), do: {-1 - arg, rest}

  defp build(2, :indefinite, rest), do: read_indefinite_chunks(rest, :bytes, [])

  defp build(2, len, rest) do
    <<bytes::binary-size(^len), rest::binary>> = rest
    {%Bytes{data: bytes}, rest}
  end

  defp build(3, :indefinite, rest), do: read_indefinite_chunks(rest, :text, [])

  defp build(3, len, rest) do
    <<text::binary-size(^len), rest::binary>> = rest
    {text, rest}
  end

  defp build(4, :indefinite, rest), do: read_indefinite_array(rest, [])
  defp build(4, len, rest), do: read_array(len, rest, [])

  defp build(5, :indefinite, rest), do: read_indefinite_map(rest, [])
  defp build(5, len, rest), do: read_map(len, rest, [])

  defp build(6, tag, rest) do
    {inner, rest} = decode_item(rest)
    {apply_tag(tag, inner), rest}
  end

  defp build(7, _arg, rest), do: {nil, rest}

  defp read_array(0, rest, acc), do: {Enum.reverse(acc), rest}

  defp read_array(count, rest, acc) do
    {item, rest} = decode_item(rest)
    read_array(count - 1, rest, [item | acc])
  end

  defp read_indefinite_array(<<0xFF, rest::binary>>, acc), do: {Enum.reverse(acc), rest}

  defp read_indefinite_array(rest, acc) do
    {item, rest} = decode_item(rest)
    read_indefinite_array(rest, [item | acc])
  end

  defp read_map(0, rest, acc), do: {Map.new(acc), rest}

  defp read_map(count, rest, acc) do
    {key, rest} = decode_item(rest)
    {value, rest} = decode_item(rest)
    read_map(count - 1, rest, [{key, value} | acc])
  end

  defp read_indefinite_map(<<0xFF, rest::binary>>, acc), do: {Map.new(acc), rest}

  defp read_indefinite_map(rest, acc) do
    {key, rest} = decode_item(rest)
    {value, rest} = decode_item(rest)
    read_indefinite_map(rest, [{key, value} | acc])
  end

  defp read_indefinite_chunks(<<0xFF, rest::binary>>, kind, acc) do
    joined = acc |> Enum.reverse() |> IO.iodata_to_binary()

    case kind do
      :bytes -> {%Bytes{data: joined}, rest}
      :text -> {joined, rest}
    end
  end

  defp read_indefinite_chunks(rest, kind, acc) do
    {chunk, rest} = decode_item(rest)
    data = if kind == :bytes, do: chunk.data, else: chunk
    read_indefinite_chunks(rest, kind, [data | acc])
  end

  defp apply_tag(0, value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> Datetime.from_datetime(datetime)
      _ -> value
    end
  end

  defp apply_tag(6, _value), do: None.none()
  defp apply_tag(7, name), do: %Table{name: name}
  defp apply_tag(8, [table, id]), do: %RecordId{table: table, id: id}
  defp apply_tag(9, value), do: Uuid.new(value)
  defp apply_tag(10, value), do: Decimal.new(value)
  defp apply_tag(12, parts), do: %Datetime{seconds: at(parts, 0), nanos: at(parts, 1)}
  defp apply_tag(13, value), do: Duration.parse(value)
  defp apply_tag(14, parts), do: Duration.from_parts(at(parts, 0), at(parts, 1))
  defp apply_tag(15, _value), do: Future.new()
  defp apply_tag(37, %Bytes{data: data}), do: Uuid.from_binary(data)
  defp apply_tag(37, data) when is_binary(data), do: Uuid.from_binary(data)

  defp apply_tag(49, [begin_bound, end_bound]) do
    %Range{begin: decode_bound(begin_bound), end: decode_bound(end_bound)}
  end

  defp apply_tag(50, value), do: {:incl, value}
  defp apply_tag(51, value), do: {:excl, value}
  defp apply_tag(88, coordinates), do: %Geometry{type: :point, coordinates: coordinates}
  defp apply_tag(89, coordinates), do: %Geometry{type: :line, coordinates: coordinates}
  defp apply_tag(90, coordinates), do: %Geometry{type: :polygon, coordinates: coordinates}
  defp apply_tag(91, coordinates), do: %Geometry{type: :multi_point, coordinates: coordinates}
  defp apply_tag(92, coordinates), do: %Geometry{type: :multi_line, coordinates: coordinates}
  defp apply_tag(93, coordinates), do: %Geometry{type: :multi_polygon, coordinates: coordinates}
  defp apply_tag(94, geometries), do: %Geometry{type: :collection, coordinates: geometries}
  defp apply_tag(_tag, inner), do: inner

  defp decode_bound(nil), do: nil
  defp decode_bound({:incl, _value} = bound), do: bound
  defp decode_bound({:excl, _value} = bound), do: bound

  defp at(list, index) when is_list(list), do: Enum.at(list, index, 0)

  defp half_to_float(bits) do
    sign = if (bits >>> 15 &&& 1) == 1, do: -1.0, else: 1.0
    exponent = bits >>> 10 &&& 0x1F
    mantissa = bits &&& 0x3FF

    cond do
      exponent == 0x1F -> 0.0
      exponent == 0 -> sign * :math.pow(2, -14) * (mantissa / 1024)
      true -> sign * :math.pow(2, exponent - 15) * (1 + mantissa / 1024)
    end
  end
end
