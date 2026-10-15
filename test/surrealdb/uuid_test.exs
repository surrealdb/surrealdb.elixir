defmodule SurrealDB.UuidTest do
  use ExUnit.Case, async: true

  alias SurrealDB.Uuid

  @format ~r/^[0-9a-f]{8}-[0-9a-f]{4}-([0-9a-f])[0-9a-f]{3}-([0-9a-f])[0-9a-f]{3}-[0-9a-f]{12}$/

  test "v4 generates random version 4 UUIDs" do
    %Uuid{value: value} = Uuid.v4()
    assert [_, "4", variant] = Regex.run(@format, value)
    assert variant in ~w(8 9 a b)
    refute Uuid.v4() == Uuid.v4()
  end

  test "v7 generates version 7 UUIDs that start with the time and sort in order" do
    before = System.system_time(:millisecond)
    %Uuid{value: value} = uuid = Uuid.v7()
    assert [_, "7", variant] = Regex.run(@format, value)
    assert variant in ~w(8 9 a b)

    <<millis::48, _::80>> = Uuid.to_binary(uuid)
    assert millis >= before and millis <= System.system_time(:millisecond)

    Process.sleep(2)
    assert Uuid.v7().value > value
  end
end
