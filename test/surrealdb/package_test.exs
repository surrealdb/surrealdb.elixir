defmodule SurrealDB.PackageTest do
  use ExUnit.Case, async: true

  @root Path.expand("../..", __DIR__)

  test "every file listed in the hex package exists" do
    for pattern <- Mix.Project.config()[:package][:files] do
      assert Path.wildcard(Path.join(@root, pattern)) != [], "missing package file: #{pattern}"
    end
  end

  test "every docs extra exists" do
    for extra <- Mix.Project.config()[:docs][:extras] do
      assert File.exists?(Path.join(@root, extra)), "missing docs extra: #{extra}"
    end
  end
end
