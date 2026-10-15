defmodule SurrealDB.VersionTest do
  use ExUnit.Case, async: true

  alias SurrealDB.Version

  doctest SurrealDB.Version

  test "accepts 2.1.0 up to, but not including, 5.0.0" do
    for version <- ~w(surrealdb-2.1.0 surrealdb-2.6.5 surrealdb-3.0.0-beta.1 surrealdb-3.3.4
                      surrealdb-3.4.0-nightly+20261014.4f7fa16 surrealdb-4.0.0 surrealdb-4.99.99) do
      assert Version.supported?(version), version
    end

    for version <- ~w(surrealdb-1.5.4 surrealdb-2.0.4 surrealdb-5.0.0 surrealdb-5.0.0-alpha.1) do
      refute Version.supported?(version), version
    end
  end

  test "explains why a version is refused" do
    assert {:error, %SurrealDB.Error{kind: :unsupported, message: message}} =
             Version.check("surrealdb-5.0.0")

    assert message =~ ">= 2.1.0 and < 5.0.0"
    assert {:error, %SurrealDB.Error{kind: :unsupported}} = Version.check("nonsense")
  end
end
