defmodule SurrealDB.SurrealQL do
  @moduledoc """
  Renders Elixir values as SurrealQL literals.

  The output follows the server's own formatter, so a rendered value parses back to the
  same value and matches what SurrealDB prints for it:

      iex> SurrealDB.SurrealQL.to_surql(SurrealDB.RecordId.new("person", "tobie"))
      "person:tobie"

      iex> SurrealDB.SurrealQL.to_surql(SurrealDB.RecordId.new("person", "complex id"))
      "person:`complex id`"

      iex> SurrealDB.SurrealQL.to_surql(%{"tags" => ["a", 1.5]})
      "{ tags: ['a', 1.5f] }"
  """

  alias SurrealDB.{Bytes, Datetime, Duration, Geometry, None, Range, RecordId, Table, Uuid}

  # Words the SurrealQL lexer may read as keywords, so they are quoted as table names.
  @reserved ~w(ALTER BEGIN BREAK CANCEL COMMIT CONTINUE CREATE DEFINE DELETE FOR IF INFO
               INSERT KILL LIVE OPTION REBUILD RETURN RELATE REMOVE SELECT LET SHOW SLEEP
               THROW UPDATE UPSERT USE DIFF RAND NONE NULL AFTER BEFORE VALUE BY ALL TRUE
               FALSE WHERE TABLE TB SEQUENCE FUNCTION)

  @doc """
  Escapes an identifier such as a table name.

  Identifiers made of ASCII letters, digits, and underscores that do not start with a
  digit and are not reserved words are left as they are. Everything else is wrapped in
  backticks.
  """
  @spec escape_ident(String.t()) :: String.t()
  def escape_ident(name) when is_binary(name) do
    if reserved?(name) or needs_quoting?(name) or starts_with_digit?(name) do
      backtick(name)
    else
      name
    end
  end

  @doc """
  Escapes a string record id key.

  Unlike a table name, a key may start with a digit or be a keyword, but a key made only of
  digits and underscores is quoted so that it does not read as a number.
  """
  @spec escape_rid_key(String.t()) :: String.t()
  def escape_rid_key(key) when is_binary(key) do
    if needs_quoting?(key) or Regex.match?(~r/^[\d_]*$/, key) do
      backtick(key)
    else
      key
    end
  end

  @doc "Renders the id part of a record id."
  @spec format_rid_key(term()) :: String.t()
  def format_rid_key(key) when is_binary(key), do: escape_rid_key(key)
  def format_rid_key(key) when is_integer(key), do: Integer.to_string(key)
  def format_rid_key(%Range{} = range), do: to_surql(range)
  def format_rid_key(key), do: to_surql(key)

  @doc "Renders any supported value as a SurrealQL literal."
  @spec to_surql(term()) :: String.t()
  def to_surql(nil), do: "NULL"
  def to_surql(%None{}), do: "NONE"
  def to_surql(true), do: "true"
  def to_surql(false), do: "false"
  def to_surql(:infinity), do: "Infinity"
  def to_surql(:neg_infinity), do: "-Infinity"
  def to_surql(:nan), do: "NaN"
  def to_surql(string) when is_binary(string), do: quote_str(string)
  def to_surql(integer) when is_integer(integer), do: Integer.to_string(integer)
  def to_surql(float) when is_float(float), do: format_float(float) <> "f"
  def to_surql(atom) when is_atom(atom), do: to_surql(Atom.to_string(atom))
  def to_surql(%Uuid{value: value}), do: "u'" <> value <> "'"

  def to_surql(%RecordId{table: table, id: id}),
    do: escape_ident(table) <> ":" <> format_rid_key(id)

  def to_surql(%Table{name: name}), do: escape_ident(name)
  def to_surql(%Decimal{} = decimal), do: format_decimal(decimal) <> "dec"
  def to_surql(%Duration{} = duration), do: Duration.to_string(duration)
  def to_surql(%Datetime{} = datetime), do: "d'" <> Datetime.to_iso8601(datetime) <> "'"
  def to_surql(%DateTime{} = datetime), do: datetime |> Datetime.from_datetime() |> to_surql()
  def to_surql(%Bytes{data: data}), do: "b\"" <> Base.encode16(data) <> "\""
  def to_surql(%Range{} = range), do: range_to_surql(range)

  def to_surql(%Geometry{type: :point, coordinates: [x, y]}),
    do: "(" <> to_number(x) <> ", " <> to_number(y) <> ")"

  def to_surql(%Geometry{} = geometry), do: geometry |> geometry_to_map() |> to_surql()

  def to_surql(list) when is_list(list), do: "[" <> Enum.map_join(list, ", ", &to_surql/1) <> "]"

  def to_surql(map) when is_map(map) and not is_struct(map) do
    if map_size(map) == 0 do
      "{  }"
    else
      pairs =
        map
        |> Enum.map(fn {key, value} -> {to_key(key), value} end)
        |> Enum.sort()
        |> Enum.map_join(", ", fn {key, value} ->
          escape_object_key(key) <> ": " <> to_surql(value)
        end)

      "{ " <> pairs <> " }"
    end
  end

  @doc """
  Quotes a string literal: single quotes unless the string contains one, in which case
  double quotes, escaping backslashes, control characters, and the quote itself.
  """
  @spec quote_str(String.t()) :: String.t()
  def quote_str(string) when is_binary(string) do
    quote = if String.contains?(string, "'"), do: "\"", else: "'"
    quote <> escape(string, quote) <> quote
  end

  @doc false
  def geometry_to_map(%Geometry{type: :collection, coordinates: geometries}) do
    %{"type" => "GeometryCollection", "geometries" => Enum.map(geometries, &geometry_to_map/1)}
  end

  def geometry_to_map(%Geometry{type: type, coordinates: coordinates}) do
    %{"type" => geojson_type(type), "coordinates" => coordinates}
  end

  # ── Helpers ────────────────────────────────────────────────────────────────

  defp reserved?(name), do: String.upcase(name) in @reserved

  defp needs_quoting?(name) do
    name == "" or name in ["NaN", "Infinity"] or not Regex.match?(~r/^[a-zA-Z0-9_]+$/, name)
  end

  defp starts_with_digit?(name), do: Regex.match?(~r/^[0-9]/, name)

  defp backtick(name), do: "`" <> escape(name, "`") <> "`"

  defp escape_object_key(key) do
    if needs_quoting?(key) or starts_with_digit?(key) do
      "\"" <> escape(key, "\"") <> "\""
    else
      key
    end
  end

  defp to_key(key) when is_binary(key), do: key
  defp to_key(key) when is_atom(key), do: Atom.to_string(key)
  defp to_key(key), do: Kernel.to_string(key)

  defp escape(string, quote) do
    for <<char::utf8 <- string>>, into: "" do
      case char do
        0 -> "\\0"
        ?\r -> "\\r"
        ?\t -> "\\t"
        ?\n -> "\\n"
        8 -> "\\u{8}"
        12 -> "\\f"
        ?\\ -> "\\\\"
        _ -> if <<char::utf8>> == quote, do: "\\" <> quote, else: <<char::utf8>>
      end
    end
  end

  defp range_to_surql(%Range{begin: begin_bound, end: end_bound}) do
    left =
      case begin_bound do
        nil -> ""
        {:incl, value} -> range_bound(value)
        {:excl, value} -> range_bound(value) <> ">"
      end

    right =
      case end_bound do
        nil -> ""
        {:incl, value} -> "=" <> range_bound(value)
        {:excl, value} -> range_bound(value)
      end

    left <> ".." <> right
  end

  defp range_bound(%Range{} = range), do: "(" <> range_to_surql(range) <> ")"
  defp range_bound(value) when is_binary(value), do: escape_rid_key(value)
  defp range_bound(value), do: to_surql(value)

  defp to_number(n) when is_integer(n), do: Integer.to_string(n)
  defp to_number(n) when is_float(n), do: format_float(n)

  # Formats a float the way Rust's `Display` does: the shortest digits that round-trip,
  # written out in full without an exponent, and without a trailing `.0`.
  defp format_float(float) do
    {sign, float} = if float < 0, do: {"-", -float}, else: {"", float}
    [mantissa, exponent] = float |> :erlang.float_to_binary([:short]) |> split_exponent()
    [int, frac] = String.split(mantissa, ".")
    digits = int <> frac
    point = String.length(int) + exponent

    plain =
      cond do
        point <= 0 ->
          "0." <> String.duplicate("0", -point) <> digits

        point >= String.length(digits) ->
          digits <> String.duplicate("0", point - String.length(digits))

        true ->
          String.slice(digits, 0, point) <> "." <> String.slice(digits, point..-1//1)
      end

    sign <> trim_number(plain)
  end

  defp split_exponent(string) do
    case String.split(string, "e") do
      [mantissa, exponent] -> [mantissa, String.to_integer(exponent)]
      [mantissa] -> [mantissa, 0]
    end
  end

  defp trim_number(plain) do
    plain =
      if String.contains?(plain, "."),
        do: plain |> String.trim_trailing("0") |> String.trim_trailing("."),
        else: plain

    case String.trim_leading(plain, "0") do
      "" -> "0"
      "." <> _ = rest -> "0" <> rest
      rest -> rest
    end
  end

  defp format_decimal(decimal) do
    decimal |> Decimal.normalize() |> Decimal.to_string(:normal)
  end

  defp geojson_type(:point), do: "Point"
  defp geojson_type(:line), do: "LineString"
  defp geojson_type(:polygon), do: "Polygon"
  defp geojson_type(:multi_point), do: "MultiPoint"
  defp geojson_type(:multi_line), do: "MultiLineString"
  defp geojson_type(:multi_polygon), do: "MultiPolygon"
end
