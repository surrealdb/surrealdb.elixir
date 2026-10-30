defmodule SurrealDB.RecordId do
  @moduledoc """
  A SurrealDB record identifier in the form `table:id`.

  The `id` part can be a string, integer, list, or map (object id). It is kept as a native
  Elixir term so it round-trips through both the CBOR and JSON protocols without loss.

      iex> SurrealDB.RecordId.new("person", "tobie")
      %SurrealDB.RecordId{table: "person", id: "tobie"}

      iex> SurrealDB.RecordId.parse!("person:tobie")
      %SurrealDB.RecordId{table: "person", id: "tobie"}
  """

  @enforce_keys [:table, :id]
  defstruct [:table, :id]

  @type id :: String.t() | integer() | list() | map()
  @type t :: %__MODULE__{table: String.t(), id: id()}

  @doc "Builds a record id from a table name and an id part."
  @spec new(String.t() | SurrealDB.Table.t(), id()) :: t()
  def new(%SurrealDB.Table{name: table}, id), do: %__MODULE__{table: table, id: id}
  def new(table, id) when is_binary(table), do: %__MODULE__{table: table, id: id}

  @doc """
  Parses a `table:id` string into a record id.

  The table is an identifier, or any text quoted with backticks or `⟨⟩`. The key is an
  identifier, an integer, quoted text, an array (`[1, 'a']`), an object (`{a: 1}`) or a
  uuid (`u'...'`), as the server prints them. Quoted text takes the SurrealQL escapes,
  `\\u{1F600}` and `\\uXXXX` included. A run of digits past the 64-bit signed range is a
  text key, as on the server.

      iex> SurrealDB.RecordId.parse("person:tobie")
      {:ok, %SurrealDB.RecordId{table: "person", id: "tobie"}}

      iex> SurrealDB.RecordId.parse("person:42")
      {:ok, %SurrealDB.RecordId{table: "person", id: 42}}

      iex> SurrealDB.RecordId.parse("person:`123`")
      {:ok, %SurrealDB.RecordId{table: "person", id: "123"}}

      iex> SurrealDB.RecordId.parse("temp:['london', 2024]")
      {:ok, %SurrealDB.RecordId{table: "temp", id: ["london", 2024]}}

      iex> match?({:error, _reason}, SurrealDB.RecordId.parse("person"))
      true
  """
  @spec parse(String.t()) :: {:ok, t()} | {:error, String.t()}
  def parse(string) when is_binary(string) do
    {table, rest} = table(string)

    rest =
      case rest do
        ":" <> rest -> rest
        _ -> fail("expected `:` after the table name in #{inspect(string)}")
      end

    case key(rest) do
      {key, ""} -> {:ok, %__MODULE__{table: table, id: key}}
      {_key, _rest} -> fail("unexpected characters after the key in #{inspect(string)}")
    end
  catch
    {__MODULE__, reason} -> {:error, reason}
  end

  @doc "Like `parse/1`, but raises `ArgumentError` when the string is not a record id."
  @spec parse!(String.t()) :: t()
  def parse!(string) do
    case parse(string) do
      {:ok, record_id} -> record_id
      {:error, reason} -> raise ArgumentError, "invalid record id: " <> reason
    end
  end

  @max_int 0x7FFFFFFFFFFFFFFF
  @min_int -0x8000000000000000

  defp fail(reason), do: throw({__MODULE__, reason})

  defp table("`" <> rest), do: quoted(rest, ?`, [])
  defp table("⟨" <> rest), do: quoted(rest, ?⟩, [])

  # An unquoted table name is an identifier: it cannot start with a digit or a minus sign.
  defp table(string) do
    case Regex.run(~r/^[a-zA-Z_][a-zA-Z0-9_]*/, string) do
      [ident] -> {ident, after_match(string, ident)}
      nil -> fail("expected an identifier at #{inspect(string)}")
    end
  end

  defp key("`" <> rest), do: quoted(rest, ?`, [])
  defp key("⟨" <> rest), do: quoted(rest, ?⟩, [])
  defp key("[" <> _ = string), do: value(string)
  defp key("{" <> _ = string), do: value(string)
  defp key("u'" <> _ = string), do: value(string)
  defp key("u\"" <> _ = string), do: value(string)

  # An unquoted key is an identifier or an integer. Digits that do not fit an integer
  # stay text.
  defp key(string) do
    case Regex.run(~r/^-?[a-zA-Z0-9_]+/, string) do
      [ident] -> {ident_key(ident), after_match(string, ident)}
      nil -> fail("expected a key at #{inspect(string)}")
    end
  end

  defp ident_key(ident) do
    with true <- Regex.match?(~r/^-?\d+$/, ident),
         integer when integer in @min_int..@max_int <- String.to_integer(ident) do
      integer
    else
      _ -> ident
    end
  end

  # The values an array or object key can hold.
  defp value(string) do
    case String.trim_leading(string) do
      "[" <> rest -> items(rest, [])
      "{" <> rest -> fields(rest, %{})
      "u'" <> rest -> uuid(quoted(rest, ?', []))
      "u\"" <> rest -> uuid(quoted(rest, ?", []))
      "'" <> rest -> quoted(rest, ?', [])
      "\"" <> rest -> quoted(rest, ?", [])
      rest -> scalar(rest)
    end
  end

  defp items(string, acc) do
    case String.trim_leading(string) do
      "]" <> rest ->
        {Enum.reverse(acc), rest}

      rest ->
        {item, rest} = value(rest)

        case String.trim_leading(rest) do
          "," <> rest -> items(rest, [item | acc])
          "]" <> rest -> {Enum.reverse([item | acc]), rest}
          _ -> fail("expected `,` or `]` at #{inspect(rest)}")
        end
    end
  end

  defp fields(string, acc) do
    case String.trim_leading(string) do
      "}" <> rest ->
        {acc, rest}

      rest ->
        {name, rest} = field_name(rest)

        {item, rest} =
          case String.trim_leading(rest) do
            ":" <> rest -> value(rest)
            _ -> fail("expected `:` after the field #{inspect(name)}")
          end

        acc = Map.put(acc, name, item)

        case String.trim_leading(rest) do
          "," <> rest -> fields(rest, acc)
          "}" <> rest -> {acc, rest}
          _ -> fail("expected `,` or `}` at #{inspect(rest)}")
        end
    end
  end

  defp field_name("'" <> rest), do: quoted(rest, ?', [])
  defp field_name("\"" <> rest), do: quoted(rest, ?", [])
  defp field_name("`" <> rest), do: quoted(rest, ?`, [])
  defp field_name("⟨" <> rest), do: quoted(rest, ?⟩, [])

  defp field_name(string) do
    case Regex.run(~r/^[a-zA-Z0-9_]+/, string) do
      [name] -> {name, after_match(string, name)}
      nil -> fail("expected a field name at #{inspect(string)}")
    end
  end

  defp scalar(string) do
    case Regex.run(~r/^(?:-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null|NULL|NONE)/, string) do
      [token] -> {scalar_value(token), after_match(string, token)}
      nil -> fail("expected a value at #{inspect(string)}")
    end
  end

  defp scalar_value("true"), do: true
  defp scalar_value("false"), do: false
  defp scalar_value(null) when null in ["null", "NULL"], do: nil
  defp scalar_value("NONE"), do: SurrealDB.None.none()

  defp scalar_value(number) do
    case Integer.parse(number) do
      {integer, ""} when integer in @min_int..@max_int -> integer
      {_integer, ""} -> fail("integer #{number} is outside the 64-bit signed range")
      _ -> number |> Float.parse() |> elem(0)
    end
  end

  defp uuid({text, rest}) do
    if Regex.match?(~r/^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$/, text),
      do: {SurrealDB.Uuid.new(text), rest},
      else: fail("#{inspect(text)} is not a uuid")
  end

  defp quoted("\\" <> rest, close, acc) do
    {char, rest} = escape(rest)
    quoted(rest, close, [char | acc])
  end

  defp quoted(<<char::utf8, rest::binary>>, close, acc) when char == close,
    do: {acc |> Enum.reverse() |> IO.iodata_to_binary(), rest}

  defp quoted(<<char::utf8, rest::binary>>, close, acc),
    do: quoted(rest, close, [<<char::utf8>> | acc])

  defp quoted(_string, close, _acc), do: fail("missing closing #{<<close::utf8>>}")

  defp escape("u{" <> rest) do
    case Regex.run(~r/^([0-9a-fA-F]{1,6})\}/, rest) do
      [match, hex] -> {code_point(String.to_integer(hex, 16)), after_match(rest, match)}
      nil -> fail("invalid \\u{...} escape")
    end
  end

  defp escape(<<"u", hex::binary-4, "\\u", low::binary-4, rest::binary>> = string) do
    with {high, ""} when high in 0xD800..0xDBFF <- Integer.parse(hex, 16),
         {low, ""} when low in 0xDC00..0xDFFF <- Integer.parse(low, 16) do
      {code_point(0x10000 + (high - 0xD800) * 0x400 + (low - 0xDC00)), rest}
    else
      _ -> escape_one(string)
    end
  end

  defp escape(string), do: escape_one(string)

  defp escape_one(<<"u", hex::binary-4, rest::binary>>) do
    case Integer.parse(hex, 16) do
      {code, ""} -> {code_point(code), rest}
      _ -> fail("invalid \\u escape")
    end
  end

  defp escape_one(<<char::utf8, rest::binary>>) do
    case char do
      ?n -> {"\n", rest}
      ?t -> {"\t", rest}
      ?r -> {"\r", rest}
      ?b -> {"\b", rest}
      ?f -> {"\f", rest}
      ?0 -> {<<0>>, rest}
      char when char in [?\\, ?', ?", ?`, ?/, ?⟩, ?⟨] -> {<<char::utf8>>, rest}
      _ -> fail("unknown escape \\#{<<char::utf8>>}")
    end
  end

  defp escape_one(_string), do: fail("unterminated escape")

  defp code_point(code) when code in 0xD800..0xDFFF,
    do: fail("escape \\u#{Integer.to_string(code, 16)} is a lone surrogate")

  defp code_point(code) when code <= 0x10FFFF, do: <<code::utf8>>
  defp code_point(_code), do: fail("escape is past the last code point")

  defp after_match(string, match),
    do: binary_part(string, byte_size(match), byte_size(string) - byte_size(match))

  @doc """
  Returns the canonical SurrealQL form, as the server prints it.

      iex> SurrealDB.RecordId.to_string(SurrealDB.RecordId.new("person", "complex id"))
      "person:`complex id`"

      iex> SurrealDB.RecordId.to_string(SurrealDB.RecordId.new("temp", ["london", 2024]))
      "temp:['london', 2024]"
  """
  @spec to_string(t()) :: String.t()
  def to_string(%__MODULE__{} = record), do: SurrealDB.SurrealQL.to_surql(record)

  defimpl String.Chars do
    def to_string(record_id), do: SurrealDB.RecordId.to_string(record_id)
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(record_id, _opts) do
      concat(["#SurrealDB.RecordId<", SurrealDB.RecordId.to_string(record_id), ">"])
    end
  end
end
