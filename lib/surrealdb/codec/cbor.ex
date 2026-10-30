defmodule SurrealDB.Codec.CBOR do
  @moduledoc """
  CBOR protocol codec with SurrealDB custom tags.

  This is the default protocol. It preserves the full set of SurrealDB value types by
  mapping each one to its custom CBOR tag:

  | Tag | Type                 | Tag | Type                 |
  | --- | -------------------- | --- | -------------------- |
  | 6   | `None`               | 50  | Range bound included |
  | 7   | `Table`              | 51  | Range bound excluded |
  | 8   | `RecordId`           | 55  | `FileRef`            |
  | 10  | `Decimal`            | 56  | Set (`MapSet`)       |
  | 12  | `Datetime`           | 88  | Geometry point       |
  | 14  | `Duration`           | 89  | Geometry line        |
  | 15  | `Future` (send only) | 90  | Geometry polygon     |
  | 37  | `Uuid`               | 91  | Geometry multi-point |
  | 49  | `Range`              | 92  | Geometry multi-line  |
  |     |                      | 93  | Geometry multi-poly  |
  |     |                      | 94  | Geometry collection  |

  A record id whose key is a range (tag 8 holding tag 49) decodes to a
  `SurrealDB.RecordIdRange`.

  This module speaks plain `cbor`, which every server version reads.
  `SurrealDB.Codec.CBOR2` speaks `cbor.2`, which SurrealDB 4.0 adds: the same encoding plus
  tags 180 to 188 for the sized numbers in `SurrealDB.Number`. Over plain `cbor` a sized
  number is sent as a plain number (a `:u64` above the 64-bit signed limit as a decimal),
  and a value tagged 180 to 188 is refused.

  Floats that the BEAM cannot represent (infinities and NaN) are decoded to the atoms
  `:infinity`, `:neg_infinity`, and `:nan`, and those atoms are encoded back as the
  matching IEEE 754 values. Floats are written at the shortest width that holds them
  exactly, as the server does.

  The codec follows the shared SurrealDB conformance vectors (surrealdb-protocol v0.14.0).
  The decoder refuses what a SurrealDB 4.0 server refuses: integers outside 64 bits signed,
  repeated or non-text map keys, the text tags 0, 9 and 13, unknown tags, and tagged values
  of the wrong shape. A refused input decodes to `{:error, %ArgumentError{}}`.

  The encoder only writes what every supported server reads: datetimes, uuids and
  durations use their binary tags (12, 37 and 14, never the text tags 0, 9 and 13),
  integers must fit in 64 bits signed, and map keys must be strings or atoms that do not
  repeat (an atom key counts as its name). A value that breaks one of these raises
  `ArgumentError`.
  """

  @behaviour SurrealDB.Codec

  import Bitwise

  alias SurrealDB.{
    Bytes,
    Datetime,
    FileRef,
    Duration,
    Future,
    Geometry,
    None,
    Number,
    Range,
    RecordId,
    RecordIdRange,
    StringRecordId,
    Table,
    Uuid
  }

  @typedoc "`:cbor` is plain `cbor`; `:cbor2` adds the sized-number tags 180 to 188."
  @type format :: :cbor | :cbor2

  @min_int -0x8000000000000000
  @max_int 0x7FFFFFFFFFFFFFFF
  @max_u64 0xFFFFFFFFFFFFFFFF

  # The datetimes the server can hold (chrono's range): -262143-01-01 to +262142-12-31.
  @min_datetime_seconds -8_334_601_228_800
  @max_datetime_seconds 8_210_266_876_799

  @sized_tags %{
    180 => :i8,
    181 => :u8,
    182 => :i16,
    183 => :u16,
    184 => :i32,
    185 => :u32,
    186 => :u64,
    187 => :f16,
    188 => :f32
  }
  @sized_kinds Map.new(@sized_tags, fn {tag, kind} -> {kind, tag} end)

  @impl true
  def content_type, do: "application/cbor"

  @impl true
  def subprotocol, do: "cbor"

  @impl true
  def encode(term), do: encode(term, :cbor)

  @impl true
  def decode(binary), do: decode(binary, :cbor)

  @doc "Encodes `term` as `format`."
  @spec encode(term(), format()) :: binary()
  def encode(term, format) when format in [:cbor, :cbor2],
    do: IO.iodata_to_binary(enc(term, format))

  @doc "Decodes one `format` item, refusing anything a SurrealDB 4.0 server refuses."
  @spec decode(binary(), format()) :: {:ok, term()} | {:error, term()}
  def decode(binary, format) when is_binary(binary) and format in [:cbor, :cbor2] do
    case parse(binary) do
      {raw, ""} -> {:ok, value(raw, format)}
      {_raw, _rest} -> invalid!("trailing bytes after the CBOR item")
    end
  rescue
    error -> {:error, error}
  end

  # ── Encoding ──────────────────────────────────────────────────────────────

  defp enc(false, _f), do: <<0xF4>>
  defp enc(true, _f), do: <<0xF5>>
  defp enc(nil, _f), do: <<0xF6>>

  defp enc(special, _f) when special in [:infinity, :neg_infinity, :nan], do: enc_float(special)

  defp enc(%None{}, _f), do: [head(6, 6), <<0xF6>>]
  defp enc(%Table{name: name}, f), do: [head(6, 7), enc(name, f)]
  defp enc(%RecordId{table: table, id: id}, f), do: [head(6, 8), enc([table, id], f)]
  defp enc(%StringRecordId{id: id}, f), do: [head(6, 8), enc(id, f)]

  defp enc(%RecordIdRange{table: table, begin: begin_bound, end: end_bound}, f),
    do: [head(6, 8), head(4, 2), enc(table, f), enc(Range.new(begin_bound, end_bound), f)]

  defp enc(%FileRef{bucket: bucket, key: key}, f), do: [head(6, 55), enc([bucket, key], f)]

  defp enc(%MapSet{} = set, f), do: [head(6, 56), enc(set |> MapSet.to_list() |> Enum.sort(), f)]

  defp enc(%Uuid{} = uuid, f), do: [head(6, 37), enc(Bytes.new(Uuid.to_binary(uuid)), f)]

  defp enc(%Decimal{} = decimal, f),
    do: [head(6, 10), enc(Decimal.to_string(decimal, :normal), f)]

  defp enc(%Datetime{seconds: seconds, nanos: nanos}, f),
    do: [head(6, 12), enc([seconds, nanos], f)]

  defp enc(%DateTime{} = datetime, f), do: enc(Datetime.from_datetime(datetime), f)

  # The server writes a zero duration as `[]` and drops zero nanoseconds. The seconds may
  # use the whole unsigned 64-bit range.
  defp enc(%Duration{nanoseconds: ns} = duration, _f) when ns < 0 do
    raise ArgumentError, "duration #{inspect(duration)} is negative; SurrealDB durations are unsigned"
  end

  defp enc(%Duration{} = duration, _f) do
    parts =
      case Duration.to_parts(duration) do
        {0, 0} -> []
        {seconds, 0} -> [seconds]
        {seconds, nanos} -> [seconds, nanos]
      end

    if hd(parts ++ [0]) > @max_u64 do
      raise ArgumentError, "duration #{inspect(duration)} is too long for SurrealDB"
    end

    [head(6, 14), head(4, length(parts)) | Enum.map(parts, &head(0, &1))]
  end

  defp enc(%Future{}, f), do: [head(6, 15), enc(nil, f)]

  defp enc(%Range{begin: begin_bound, end: end_bound}, f) do
    [head(6, 49), head(4, 2), enc_bound(begin_bound, f), enc_bound(end_bound, f)]
  end

  defp enc(%Geometry{} = geometry, _f), do: enc_geometry(geometry)

  defp enc(%Number{kind: kind, value: value}, :cbor2) when kind in [:f16, :f32],
    do: [head(6, @sized_kinds[kind]), enc_float(value)]

  defp enc(%Number{kind: kind, value: value}, :cbor2),
    do: [head(6, @sized_kinds[kind]), enc_int(value)]

  # Plain cbor has no sized numbers, so they widen as the server widens them.
  defp enc(%Number{kind: kind, value: value}, :cbor) when kind in [:f16, :f32],
    do: enc_float(value)

  defp enc(%Number{value: value}, :cbor) when value > @max_int,
    do: [head(6, 10), enc(Integer.to_string(value), :cbor)]

  defp enc(%Number{value: value}, :cbor), do: enc_int(value)

  defp enc(%Bytes{data: data}, _f), do: [head(2, byte_size(data)), data]

  # SurrealDB integers are 64-bit signed. A bigger one would be written as an unsigned or
  # truncated CBOR integer, which an older server reads as a different number and a newer
  # one refuses, so it is rejected here. Send such a value as a `Decimal`.
  defp enc(integer, _f) when is_integer(integer) and (integer > @max_int or integer < @min_int) do
    raise ArgumentError,
          "integer #{integer} is outside the 64-bit signed range SurrealDB accepts; " <>
            "send it as a Decimal"
  end

  defp enc(integer, _f) when is_integer(integer), do: enc_int(integer)
  defp enc(float, _f) when is_float(float), do: enc_float(float)

  defp enc(atom, f) when is_atom(atom), do: enc(Atom.to_string(atom), f)

  defp enc(binary, _f) when is_binary(binary), do: [head(3, byte_size(binary)), binary]

  defp enc(list, f) when is_list(list) do
    [head(4, length(list)) | Enum.map(list, &enc(&1, f))]
  end

  # Keys are written sorted, as the server writes them.
  defp enc(map, f) when is_map(map) and not is_struct(map) do
    pairs = map |> Enum.map(fn {key, value} -> {map_key(key), value} end) |> Enum.sort()
    check_unique_keys!(pairs)

    [
      head(5, map_size(map))
      | Enum.map(pairs, fn {key, value} -> [enc(key, f), enc(value, f)] end)
    ]
  end

  defp enc(other, _f), do: raise(ArgumentError, "cannot encode #{inspect(other)} as CBOR")

  defp enc_int(integer) when integer >= 0, do: head(0, integer)
  defp enc_int(integer), do: head(1, -1 - integer)

  defp enc_float(:infinity), do: <<0xF9, 0x7C00::16>>
  defp enc_float(:neg_infinity), do: <<0xF9, 0xFC00::16>>
  defp enc_float(:nan), do: <<0xF9, 0x7E00::16>>

  defp enc_float(float) do
    cond do
      Number.exact?(:f16, float) -> <<0xF9, float::float-16>>
      Number.exact?(:f32, float) -> <<0xFA, float::float-32>>
      true -> <<0xFB, float::float-64>>
    end
  end

  # An atom key is written as its name, so `%{a: 1, "a" => 2}` would put the key twice,
  # which a newer server refuses and an older one resolves to one of the values. The pairs
  # are sorted, so a repeat sits next to its twin.
  defp check_unique_keys!(pairs) do
    pairs
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.each(fn
      [{key, _}, {key, _}] ->
        raise ArgumentError, "duplicate map key #{inspect(key)} once atom keys are strings"

      _ ->
        :ok
    end)
  end

  defp map_key(key) when is_binary(key), do: key
  defp map_key(key) when is_atom(key) and key not in [nil, true, false], do: Atom.to_string(key)

  defp map_key(key) do
    raise ArgumentError, "map key #{inspect(key)} is not a string; SurrealDB keys are text"
  end

  defp enc_bound(nil, f), do: enc(nil, f)
  defp enc_bound({:incl, value}, f), do: [head(6, 50), enc(value, f)]
  defp enc_bound({:excl, value}, f), do: [head(6, 51), enc(value, f)]

  # Each geometry nests the tagged geometries it is made of: a line holds tagged points, a
  # polygon tagged lines, and so on.
  defp enc_geometry(%Geometry{type: :point, coordinates: [x, y]}),
    do: [head(6, 88), head(4, 2), enc_coordinate(x), enc_coordinate(y)]

  defp enc_geometry(%Geometry{type: :collection, coordinates: geometries}),
    do: [head(6, 94), head(4, length(geometries)) | Enum.map(geometries, &enc_geometry/1)]

  defp enc_geometry(%Geometry{type: type, coordinates: parts}) do
    part_type = geometry_part(type)
    parts = Enum.map(parts, &enc_geometry(%Geometry{type: part_type, coordinates: &1}))
    [head(6, geometry_tag(type)), head(4, length(parts)) | parts]
  end

  defp enc_coordinate(number) when is_integer(number), do: enc_float(number / 1)
  defp enc_coordinate(number), do: enc(number, :cbor)

  defp geometry_part(:line), do: :point
  defp geometry_part(:polygon), do: :line
  defp geometry_part(:multi_point), do: :point
  defp geometry_part(:multi_line), do: :line
  defp geometry_part(:multi_polygon), do: :polygon

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

  # ── Decoding, pass 1: CBOR items ──────────────────────────────────────────
  #
  # The first pass reads the CBOR structure only: integers of any size, floats (with the
  # special atoms), `true`/`false`/`nil`, text, `%Bytes{}`, lists, `{:map, pairs}` and
  # `{:tag, tag, item}`. The second pass checks each item in context: an integer past
  # 64 bits signed is refused unless a tag allows it, as tag 186 and the duration seconds do.

  defp parse(<<0xF4, rest::binary>>), do: {false, rest}
  defp parse(<<0xF5, rest::binary>>), do: {true, rest}
  defp parse(<<0xF6, rest::binary>>), do: {nil, rest}
  defp parse(<<0xF7, rest::binary>>), do: {nil, rest}
  defp parse(<<0xF9, bits::bits-16, rest::binary>>), do: {read_float(bits), rest}
  defp parse(<<0xFA, bits::bits-32, rest::binary>>), do: {read_float(bits), rest}
  defp parse(<<0xFB, bits::bits-64, rest::binary>>), do: {read_float(bits), rest}

  defp parse(<<major::3, info::5, rest::binary>>) when major != 7 do
    {arg, rest} = read_arg(info, rest)
    item(major, arg, rest)
  end

  defp parse(<<byte, _::binary>>), do: invalid!("unsupported CBOR simple value 0x#{hex(byte)}")
  defp parse(<<>>), do: invalid!("unexpected end of CBOR input")

  defp read_arg(info, rest) when info < 24, do: {info, rest}
  defp read_arg(24, <<arg, rest::binary>>), do: {arg, rest}
  defp read_arg(25, <<arg::16, rest::binary>>), do: {arg, rest}
  defp read_arg(26, <<arg::32, rest::binary>>), do: {arg, rest}
  defp read_arg(27, <<arg::64, rest::binary>>), do: {arg, rest}
  defp read_arg(31, rest), do: {:indefinite, rest}
  defp read_arg(_info, _rest), do: invalid!("malformed CBOR item header")

  defp item(0, :indefinite, _rest), do: invalid!("an integer cannot have indefinite length")
  defp item(0, arg, rest), do: {arg, rest}
  defp item(1, :indefinite, _rest), do: invalid!("an integer cannot have indefinite length")
  defp item(1, arg, rest), do: {-1 - arg, rest}

  defp item(2, :indefinite, rest) do
    {data, rest} = chunks(rest, 2, [])
    {%Bytes{data: data}, rest}
  end

  defp item(2, len, rest), do: {%Bytes{data: take(rest, len)}, skip(rest, len)}

  # Each chunk of an indefinite-length text must be definite-length text that is valid
  # UTF-8 on its own, so a chunk boundary cannot split a character.
  defp item(3, :indefinite, rest), do: chunks(rest, 3, [])
  defp item(3, len, rest), do: {text!(take(rest, len)), skip(rest, len)}

  defp item(4, :indefinite, rest), do: indefinite_array(rest, [])
  defp item(4, len, rest), do: array(len, rest, [])

  defp item(5, :indefinite, rest), do: indefinite_map(rest, [])
  defp item(5, len, rest), do: map(len, rest, [])

  defp item(6, :indefinite, _rest), do: invalid!("a tag cannot have indefinite length")

  defp item(6, tag, rest) do
    {inner, rest} = parse(rest)
    {{:tag, tag, inner}, rest}
  end

  defp take(rest, len) when byte_size(rest) >= len, do: binary_part(rest, 0, len)
  defp take(_rest, _len), do: invalid!("unexpected end of CBOR input")

  defp skip(rest, len), do: binary_part(rest, len, byte_size(rest) - len)

  defp chunks(<<0xFF, rest::binary>>, _major, acc),
    do: {acc |> Enum.reverse() |> IO.iodata_to_binary(), rest}

  defp chunks(<<major::3, info::5, rest::binary>>, major, acc) when info < 31 do
    {len, rest} = read_arg(info, rest)
    chunk = take(rest, len)
    if major == 3, do: text!(chunk)
    chunks(skip(rest, len), major, [chunk | acc])
  end

  defp chunks(_rest, _major, _acc),
    do: invalid!("an indefinite-length string holds a chunk of another type")

  defp text!(text) do
    if String.valid?(text), do: text, else: invalid!("CBOR text is not valid UTF-8")
  end

  defp array(0, rest, acc), do: {Enum.reverse(acc), rest}

  defp array(count, rest, acc) do
    {item, rest} = parse(rest)
    array(count - 1, rest, [item | acc])
  end

  defp indefinite_array(<<0xFF, rest::binary>>, acc), do: {Enum.reverse(acc), rest}

  defp indefinite_array(rest, acc) do
    {item, rest} = parse(rest)
    indefinite_array(rest, [item | acc])
  end

  defp map(0, rest, acc), do: {{:map, Enum.reverse(acc)}, rest}

  defp map(count, rest, acc) do
    {key, rest} = parse(rest)
    {value, rest} = parse(rest)
    map(count - 1, rest, [{key, value} | acc])
  end

  defp indefinite_map(<<0xFF, rest::binary>>, acc), do: {{:map, Enum.reverse(acc)}, rest}

  defp indefinite_map(rest, acc) do
    {key, rest} = parse(rest)
    {value, rest} = parse(rest)
    indefinite_map(rest, [{key, value} | acc])
  end

  # The BEAM has no infinity or NaN floats, and a binary float match fails on them, so
  # the special values are recognised from the exponent bits first.
  defp read_float(<<sign::1, 0x1F::5, mantissa::10>>), do: special(sign, mantissa)
  defp read_float(<<sign::1, 0xFF::8, mantissa::23>>), do: special(sign, mantissa)
  defp read_float(<<sign::1, 0x7FF::11, mantissa::52>>), do: special(sign, mantissa)
  defp read_float(<<float::float-16>>), do: float
  defp read_float(<<float::float-32>>), do: float
  defp read_float(<<float::float-64>>), do: float

  defp special(_sign, mantissa) when mantissa != 0, do: :nan
  defp special(0, 0), do: :infinity
  defp special(1, 0), do: :neg_infinity

  # ── Decoding, pass 2: SurrealDB values ────────────────────────────────────

  defp value(integer, _f) when is_integer(integer) and integer in @min_int..@max_int, do: integer

  defp value(integer, _f) when is_integer(integer),
    do: invalid!("integer #{integer} is outside the 64-bit signed range")

  defp value(list, f) when is_list(list), do: Enum.map(list, &value(&1, f))
  defp value({:map, pairs}, f), do: object(pairs, f)
  defp value({:tag, tag, inner}, f), do: tagged(tag, inner, f)
  defp value(other, _f), do: other

  defp object(pairs, f) do
    Enum.reduce(pairs, %{}, fn
      {key, value}, acc when is_binary(key) ->
        if Map.has_key?(acc, key), do: invalid!("duplicate map key #{inspect(key)}")
        Map.put(acc, key, value(value, f))

      {key, _value}, _acc ->
        invalid!("map key #{inspect(key)} is not text")
    end)
  end

  defp tagged(6, nil, _f), do: None.none()
  defp tagged(7, name, _f) when is_binary(name), do: %Table{name: name}

  defp tagged(8, text, _f) when is_binary(text) do
    case RecordId.parse(text) do
      {:ok, %RecordId{table: table, id: %Range{begin: b, end: e}}} ->
        %RecordIdRange{table: table, begin: b, end: e}

      {:ok, record} ->
        record

      {:error, reason} ->
        invalid!("invalid record id text: " <> reason)
    end
  end

  defp tagged(8, [table, {:tag, 49, bounds}], f) when is_binary(table) do
    %Range{begin: begin_bound, end: end_bound} = range(bounds, &record_key(&1, f))
    %RecordIdRange{table: table, begin: begin_bound, end: end_bound}
  end

  defp tagged(8, [table, key], f) when is_binary(table),
    do: %RecordId{table: table, id: record_key(key, f)}

  defp tagged(10, text, _f) when is_binary(text), do: decimal(text)

  defp tagged(12, [seconds, nanos], _f)
       when is_integer(seconds) and seconds in @min_datetime_seconds..@max_datetime_seconds and
              is_integer(nanos) and nanos in 0..999_999_999,
       do: %Datetime{seconds: seconds, nanos: nanos}

  defp tagged(14, parts, _f) when is_list(parts) and length(parts) <= 2 do
    case parts ++ Enum.drop([0, 0], length(parts)) do
      [seconds, nanos]
      when is_integer(seconds) and seconds in 0..@max_u64 and is_integer(nanos) and
             nanos in 0..0xFFFFFFFF and seconds + div(nanos, 1_000_000_000) <= @max_u64 ->
        Duration.from_parts(seconds, nanos)

      _ ->
        invalid!("tag 14 (duration) needs non-negative seconds and nanoseconds")
    end
  end

  defp tagged(37, %Bytes{data: <<_::binary-16>> = data}, _f), do: Uuid.from_binary(data)
  defp tagged(49, bounds, f), do: range(bounds, &value(&1, f))

  defp tagged(55, [bucket, key], _f) when is_binary(bucket) and is_binary(key),
    do: %FileRef{bucket: bucket, key: key}

  defp tagged(56, items, f) when is_list(items), do: MapSet.new(items, &value(&1, f))

  defp tagged(tag, inner, f) when tag in 88..94, do: geometry(tag, inner, f)

  defp tagged(tag, inner, :cbor2) when is_map_key(@sized_tags, tag),
    do: sized(@sized_tags[tag], inner)

  defp tagged(tag, _inner, :cbor) when is_map_key(@sized_tags, tag),
    do: invalid!("tag #{tag} (a sized number) needs the cbor.2 protocol")

  defp tagged(tag, inner, _f) when tag in [6, 7, 8, 10, 12, 14, 37, 49, 55, 56],
    do: invalid!("tag #{tag} holds #{describe(inner)}, which is not its shape")

  defp tagged(tag, _inner, _f), do: invalid!("CBOR tag #{tag} is not a SurrealDB value")

  defp range([lower, upper], bound_value),
    do: %Range{begin: bound(lower, bound_value), end: bound(upper, bound_value)}

  defp range(_bounds, _bound_value), do: invalid!("tag 49 (range) needs two bounds")

  defp bound(nil, _bound_value), do: nil
  defp bound({:tag, 50, value}, bound_value), do: {:incl, bound_value.(value)}
  defp bound({:tag, 51, value}, bound_value), do: {:excl, bound_value.(value)}
  defp bound(other, _bound_value), do: invalid!("#{describe(other)} is not a range bound")

  # A record id key is an integer, text, an array, an object, a uuid or a range. A sized
  # integer key is a plain integer key; nested inside an array or object it keeps its kind.
  defp record_key(key, _f) when is_binary(key), do: key
  defp record_key(key, f) when is_integer(key), do: value(key, f)
  defp record_key(key, f) when is_list(key), do: value(key, f)
  defp record_key({:map, _} = key, f), do: value(key, f)
  defp record_key({:tag, 37, _} = key, f), do: value(key, f)

  defp record_key({:tag, tag, inner}, :cbor2) when tag in 180..186,
    do: value(sized(@sized_tags[tag], inner).value, :cbor2)

  defp record_key(key, _f), do: invalid!("#{describe(key)} cannot be a record id key")

  defp sized(kind, value) when kind in [:f16, :f32] do
    if (is_float(value) or value in [:infinity, :neg_infinity, :nan]) and
         Number.exact?(kind, value),
       do: %Number{kind: kind, value: value},
       else: invalid!("#{inspect(value)} is not an exact #{kind}")
  end

  defp sized(kind, value) when is_integer(value) do
    Number.new(kind, value)
  rescue
    ArgumentError -> invalid!("#{value} does not fit in #{kind}")
  end

  defp sized(kind, value), do: invalid!("#{inspect(value)} is not a #{kind}")

  # A decimal is digits with an optional fraction: a 96-bit mantissa and at most 28
  # fractional digits. The server drops trailing fractional zeros.
  defp decimal(text) do
    case Regex.run(~r/^(-?)(\d+)(?:\.(\d+))?$/, text) do
      [_, sign, whole, fraction] ->
        fraction = String.trim_trailing(fraction, "0")

        if String.length(fraction) > 28 or String.to_integer(whole <> fraction) >= 1 <<< 96,
          do: invalid!("decimal #{text} is outside the range SurrealDB holds")

        Decimal.new(sign <> whole <> if(fraction == "", do: "", else: "." <> fraction))

      [_, sign, whole] ->
        if String.to_integer(whole) >= 1 <<< 96,
          do: invalid!("decimal #{text} is outside the range SurrealDB holds")

        Decimal.new(sign <> whole)

      nil ->
        invalid!("#{inspect(text)} is not a decimal")
    end
  end

  # Geometries nest: a line holds points, a polygon lines, and so on. The struct keeps the
  # bare coordinates, so the nested geometries are unwrapped.
  defp geometry(88, [x, y], f),
    do: %Geometry{type: :point, coordinates: [coordinate(x, f), coordinate(y, f)]}

  defp geometry(89, points, f) when is_list(points),
    do: %Geometry{type: :line, coordinates: parts(points, :point, f)}

  defp geometry(90, [_ | _] = lines, f) do
    rings = lines |> parts(:line, f) |> Enum.map(&close_ring/1)
    %Geometry{type: :polygon, coordinates: rings}
  end

  defp geometry(91, points, f) when is_list(points),
    do: %Geometry{type: :multi_point, coordinates: parts(points, :point, f)}

  defp geometry(92, lines, f) when is_list(lines),
    do: %Geometry{type: :multi_line, coordinates: parts(lines, :line, f)}

  defp geometry(93, polygons, f) when is_list(polygons),
    do: %Geometry{type: :multi_polygon, coordinates: parts(polygons, :polygon, f)}

  defp geometry(94, geometries, f) when is_list(geometries) do
    geometries =
      Enum.map(geometries, fn item ->
        case value(item, f) do
          %Geometry{} = geometry -> geometry
          other -> invalid!("#{inspect(other)} is not a geometry")
        end
      end)

    %Geometry{type: :collection, coordinates: geometries}
  end

  defp geometry(tag, inner, _f), do: invalid!("tag #{tag} (geometry) holds #{describe(inner)}")

  defp parts(items, type, f) do
    Enum.map(items, fn item ->
      case value(item, f) do
        %Geometry{type: ^type, coordinates: coordinates} -> coordinates
        other -> invalid!("#{inspect(other)} is not a geometry #{type}")
      end
    end)
  end

  defp coordinate(number, f) do
    case value(number, f) do
      integer when is_integer(integer) -> integer / 1
      %Number{value: value} when is_integer(value) -> value / 1
      %Number{value: value} -> value
      float when is_float(float) or float in [:infinity, :neg_infinity, :nan] -> float
      other -> invalid!("#{inspect(other)} is not a coordinate")
    end
  end

  defp close_ring([first | _] = ring) do
    if List.last(ring) == first, do: ring, else: ring ++ [first]
  end

  defp close_ring([]), do: []

  defp hex(byte), do: Integer.to_string(byte, 16)

  # Names a pass-1 item in an error message.
  defp describe({:tag, tag, _inner}), do: "a tag #{tag} value"
  defp describe({:map, _pairs}), do: "an object"
  defp describe(list) when is_list(list), do: "[" <> Enum.map_join(list, ", ", &describe/1) <> "]"
  defp describe(other), do: inspect(other)

  defp invalid!(message), do: raise(ArgumentError, message)
end
