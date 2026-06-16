defmodule SurrealDB.Auth do
  @moduledoc """
  Credentials for `SurrealDB.signin/2` and `SurrealDB.signup/2`.

  Pick the struct that matches the access level:

    * `SurrealDB.Auth.Root` - system user (no namespace or database).
    * `SurrealDB.Auth.Namespace` - namespace user.
    * `SurrealDB.Auth.Database` - database user.
    * `SurrealDB.Auth.Record` - record access (scope) sign in or sign up, with arbitrary
      additional variables passed to the access method.

  Each struct is turned into the parameter map SurrealDB expects by `to_params/1`.
  """

  defmodule Root do
    @moduledoc "Root (system) credentials."
    @enforce_keys [:user, :pass]
    defstruct [:user, :pass]
    @type t :: %__MODULE__{user: String.t(), pass: String.t()}
  end

  defmodule Namespace do
    @moduledoc "Namespace level credentials."
    @enforce_keys [:namespace, :user, :pass]
    defstruct [:namespace, :user, :pass]
    @type t :: %__MODULE__{namespace: String.t(), user: String.t(), pass: String.t()}
  end

  defmodule Database do
    @moduledoc "Database level credentials."
    @enforce_keys [:namespace, :database, :user, :pass]
    defstruct [:namespace, :database, :user, :pass]

    @type t :: %__MODULE__{
            namespace: String.t(),
            database: String.t(),
            user: String.t(),
            pass: String.t()
          }
  end

  defmodule Record do
    @moduledoc """
    Record access credentials (scopes). `variables` holds any extra fields the access
    method's `SIGNIN`/`SIGNUP` query expects (for example `email` and `password`).
    """
    @enforce_keys [:namespace, :database, :access]
    defstruct [:namespace, :database, :access, variables: %{}]

    @type t :: %__MODULE__{
            namespace: String.t(),
            database: String.t(),
            access: String.t(),
            variables: map()
          }
  end

  @type t :: Root.t() | Namespace.t() | Database.t() | Record.t()

  @doc "Converts an auth struct into the parameter map for the `signin`/`signup` RPC."
  @spec to_params(t()) :: map()
  def to_params(%Root{user: user, pass: pass}), do: %{"user" => user, "pass" => pass}

  def to_params(%Namespace{namespace: ns, user: user, pass: pass}) do
    %{"ns" => ns, "user" => user, "pass" => pass}
  end

  def to_params(%Database{namespace: ns, database: db, user: user, pass: pass}) do
    %{"ns" => ns, "db" => db, "user" => user, "pass" => pass}
  end

  def to_params(%Record{namespace: ns, database: db, access: access, variables: variables}) do
    Map.merge(%{"ns" => ns, "db" => db, "ac" => access}, stringify(variables))
  end

  defp stringify(map) do
    Map.new(map, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), value}
      {key, value} -> {key, value}
    end)
  end
end
