defmodule SurrealDB.AuthTest do
  use ExUnit.Case, async: true

  alias SurrealDB.Auth

  test "root credentials" do
    assert Auth.to_params(%Auth.Root{user: "root", pass: "root"}) == %{
             "user" => "root",
             "pass" => "root"
           }
  end

  test "namespace credentials" do
    auth = %Auth.Namespace{namespace: "test", user: "u", pass: "p"}
    assert Auth.to_params(auth) == %{"ns" => "test", "user" => "u", "pass" => "p"}
  end

  test "database credentials" do
    auth = %Auth.Database{namespace: "test", database: "app", user: "u", pass: "p"}
    assert Auth.to_params(auth) == %{"ns" => "test", "db" => "app", "user" => "u", "pass" => "p"}
  end

  test "record access merges extra variables" do
    auth = %Auth.Record{
      namespace: "test",
      database: "app",
      access: "user",
      variables: %{email: "a@b.com", password: "secret"}
    }

    assert Auth.to_params(auth) == %{
             "ns" => "test",
             "db" => "app",
             "ac" => "user",
             "email" => "a@b.com",
             "password" => "secret"
           }
  end
end
