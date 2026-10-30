defmodule SurrealDB.NumberTest do
  use ExUnit.Case, async: true

  doctest SurrealDB.Number

  alias SurrealDB.Codec.{CBOR, CBOR2, JSON}
  alias SurrealDB.{Number, RecordId, SurrealQL}

  describe "building" do
    test "integer kinds check their range" do
      assert Number.i8(-128).value == -128
      assert Number.u64(18_446_744_073_709_551_615).value == 18_446_744_073_709_551_615

      for {kind, value} <- [
            i8: 128,
            u8: -1,
            i16: 32_768,
            u16: 65_536,
            i32: 2_147_483_648,
            u64: -1
          ] do
        assert_raise ArgumentError, ~r/does not fit/, fn -> Number.new(kind, value) end
      end
    end

    test "float kinds round to their precision and refuse what overflows it" do
      assert Number.f16(1.5).value == 1.5
      assert Number.f16(0.1).value == 0.0999755859375
      assert Number.f32(2).value == 2.0
      assert Number.f16(:nan).value == :nan
      assert Number.f16(-0.0).value === -0.0

      assert_raise ArgumentError, ~r/too large for f16/, fn -> Number.f16(70_000) end
      assert_raise ArgumentError, ~r/too large for f32/, fn -> Number.f32(1.0e300) end
    end

    test "a value of the wrong type is refused" do
      assert_raise ArgumentError, fn -> Number.u8(1.0) end
      assert_raise ArgumentError, fn -> Number.f32("1") end
      assert_raise ArgumentError, fn -> Number.new(:u128, 1) end
    end
  end

  describe "over cbor.2" do
    test "each kind is its own tag, and decodes back to the same kind" do
      for {number, tag} <- [
            {Number.i8(-1), 180},
            {Number.u8(1), 181},
            {Number.i16(-1), 182},
            {Number.u16(1), 183},
            {Number.i32(-1), 184},
            {Number.u32(1), 185},
            {Number.u64(1), 186},
            {Number.f16(1.5), 187},
            {Number.f32(1.5), 188}
          ] do
        assert <<0xD8, ^tag, _::binary>> = encoded = CBOR2.encode(number)
        assert CBOR2.decode(encoded) == {:ok, number}
      end
    end

    test "a u64 past the 64-bit signed range keeps its value" do
      number = Number.u64(18_446_744_073_709_551_615)
      assert CBOR2.decode(CBOR2.encode(number)) == {:ok, number}
    end

    test "a sized record id key decodes to a plain integer" do
      encoded = CBOR2.encode(RecordId.new("t", Number.u8(5)))
      assert CBOR2.decode(encoded) == {:ok, RecordId.new("t", 5)}
    end
  end

  describe "over plain cbor" do
    test "sized numbers widen to plain numbers" do
      assert CBOR.encode(Number.u8(5)) == CBOR.encode(5)
      assert CBOR.encode(Number.f32(1.5)) == CBOR.encode(1.5)
      assert CBOR.encode(Number.f16(:infinity)) == CBOR.encode(:infinity)
    end

    test "a u64 past the 64-bit signed range widens to a decimal" do
      encoded = CBOR.encode(Number.u64(18_446_744_073_709_551_615))
      assert CBOR.decode(encoded) == {:ok, Decimal.new("18446744073709551615")}
    end

    test "the sized-number tags are refused" do
      assert {:error, %ArgumentError{message: message}} = CBOR.decode(CBOR2.encode(Number.u8(5)))
      assert message =~ "cbor.2"
    end
  end

  test "JSON sends the plain number" do
    assert JSON.encode(%{"n" => Number.u16(7)}) |> IO.iodata_to_binary() == ~s({"n":7})
  end

  test "SurrealQL writes the kind as a suffix" do
    assert SurrealQL.to_surql(Number.u8(5)) == "5u8"
    assert SurrealQL.to_surql(Number.f32(1.5)) == "1.5f32"
    assert to_string(Number.i8(1)) == "1i8"
  end
end
