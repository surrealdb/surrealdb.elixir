defmodule SurrealDB.Connection.Session do
  @moduledoc false
  # What the connection knows about one server-side session, so it can send the right
  # HTTP headers and restore the session after a reconnect.
  defstruct [
    :namespace,
    :database,
    :token,
    :refresh_token,
    # signin params to sign in again with when the token can no longer be used
    :credentials,
    vars: %{}
  ]
end
