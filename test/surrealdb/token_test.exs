defmodule SurrealDB.TokenTest do
  use ExUnit.Case, async: true

  @moduletag :capture_log

  alias SurrealDB.Connection

  # A JWT whose claims say it was issued now and expires in `seconds`.
  defp jwt(seconds, tag \\ "a") do
    now = System.system_time(:second)

    claims =
      Base.url_encode64(Jason.encode!(%{iat: now, exp: now + seconds, tag: tag}), padding: false)

    "header." <> claims <> ".signature"
  end

  # Reports every RPC; `replies` maps a method to a reply or to a list of replies used in turn.
  defp responder(parent, replies) do
    fn
      "__connect__", _params ->
        :ok

      method, params ->
        send(parent, {:rpc, method, params})

        case Map.get(replies, method) do
          replies when is_list(replies) ->
            # Each call takes the next reply; the last one repeats.
            n = Process.get({:calls, method}, 0)
            Process.put({:calls, method}, n + 1)
            Enum.at(replies, n, List.last(replies))

          nil ->
            if method == "version", do: {:ok, "surrealdb-3.3.4"}, else: {:ok, nil}

          reply ->
            reply
        end
    end
  end

  defp start(replies, opts \\ [], id \\ Connection) do
    base = [
      url: "ws://mock/rpc",
      engine: SurrealDB.Engine.Mock,
      responder: responder(self(), replies),
      reconnect: [delay: 10, max_delay: 50]
    ]

    start_supervised!({Connection, Keyword.merge(base, opts)}, id: id)
  end

  @record %SurrealDB.Auth.Record{namespace: "n", database: "d", access: "account"}

  test "the renewal delay leaves a minute, or half of a shorter lifetime" do
    assert {:ok, delay} = Connection.renewal_delay(jwt(3600))
    assert_in_delta delay, 3_540_000, 2_000

    assert {:ok, delay} = Connection.renewal_delay(jwt(10))
    assert_in_delta delay, 5_000, 2_000

    assert :error = Connection.renewal_delay("not-a-jwt")
  end

  test "refresh exchanges the stored token pair and keeps the new one" do
    pair = %{"access" => "access.1", "refresh" => "refresh.1"}
    new_pair = %{"access" => "access.2", "refresh" => "refresh.2"}
    db = start(%{"signin" => {:ok, pair}, "refresh" => {:ok, new_pair}})

    {:ok, _} = SurrealDB.signin(db, @record)
    assert {:ok, ^new_pair} = SurrealDB.refresh(db)
    assert_received {:rpc, "refresh", [^pair]}

    assert %{token: "access.2", refresh_token: "refresh.2"} = SurrealDB.info_session(db)
  end

  test "refresh without a refresh token fails without a round trip" do
    db = start(%{"signin" => {:ok, "access"}})
    {:ok, _} = SurrealDB.signin(db, @record)

    assert {:error, %SurrealDB.Error{kind: :auth}} = SurrealDB.refresh(db)
    refute_received {:rpc, "refresh", _}
  end

  test "revoke sends the stored pair and forgets the refresh token" do
    pair = %{"access" => "access.1", "refresh" => "refresh.1"}
    db = start(%{"signin" => {:ok, pair}})
    {:ok, _} = SurrealDB.signin(db, @record)

    assert {:ok, _} = SurrealDB.revoke(db)
    assert_received {:rpc, "revoke", [^pair]}
    assert %{token: "access.1", refresh_token: nil} = SurrealDB.info_session(db)
  end

  test "authenticate with a token pair sends the access token and keeps the refresh token" do
    db = start(%{})

    assert {:ok, _} = SurrealDB.authenticate(db, %{access: "access.1", refresh: "refresh.1"})
    assert_received {:rpc, "authenticate", ["access.1"]}
    assert %{token: "access.1", refresh_token: "refresh.1"} = SurrealDB.info_session(db)
  end

  test "renews the access token with the refresh token before it expires" do
    first = jwt(2, "first")
    pair = %{"access" => first, "refresh" => "refresh.1"}
    new_pair = %{"access" => jwt(3600, "second"), "refresh" => "refresh.2"}
    db = start(%{"signin" => {:ok, pair}, "refresh" => {:ok, new_pair}})

    {:ok, _} = SurrealDB.signin(db, @record)
    assert_receive {:rpc, "refresh", [^pair]}, 3_000

    assert %{refresh_token: "refresh.2"} = SurrealDB.info_session(db)
  end

  test "renews by signing in again when there is no refresh token" do
    db = start(%{"signin" => [{:ok, jwt(2, "first")}, {:ok, jwt(3600, "second")}]})
    auth = %SurrealDB.Auth.Root{user: "root", pass: "root"}

    {:ok, _} = SurrealDB.signin(db, auth)
    assert_received {:rpc, "signin", [credentials]}
    assert_receive {:rpc, "signin", [^credentials]}, 3_000
  end

  test "falls back to signing in when the refresh fails" do
    pair = %{"access" => jwt(2), "refresh" => "refresh.1"}
    error = %{"code" => -32_002, "message" => "Invalid refresh token", "kind" => "NotAllowed"}

    db =
      start(%{
        "signin" => [{:ok, pair}, {:ok, jwt(3600)}],
        "refresh" => {:error, error}
      })

    {:ok, _} = SurrealDB.signin(db, @record)
    assert_receive {:rpc, "refresh", _}, 3_000
    assert_receive {:rpc, "signin", _}, 1_000
    assert %{refresh_token: nil} = SurrealDB.info_session(db)
  end

  test "a failed renewal is tried again after a backoff, not at once" do
    error = %{"code" => -32_002, "message" => "Not allowed", "kind" => "NotAllowed"}
    db = start(%{"signin" => [{:ok, jwt(1)}, {:error, error}]})

    {:ok, _} = SurrealDB.signin(db, %SurrealDB.Auth.Root{user: "root", pass: "root"})
    assert_received {:rpc, "signin", _}
    assert_receive {:rpc, "signin", _}, 2_000

    # The first retry waits a second.
    refute_receive {:rpc, "signin", _}, 700
    assert_receive {:rpc, "signin", _}, 2_000
  end

  test "revoking the session's refresh token stops renewal" do
    pair = %{"access" => jwt(2), "refresh" => "refresh.1"}
    db = start(%{"signin" => {:ok, pair}})

    {:ok, _} = SurrealDB.signin(db, @record)
    {:ok, _} = SurrealDB.revoke(db)
    assert_received {:rpc, "signin", _}

    refute_receive {:rpc, "signin", _}, 2_000
    refute_received {:rpc, "refresh", _}
    assert :sys.get_state(db).renewals == %{}
  end

  test "a refresh that fails for another reason keeps the pair and is tried again" do
    new_pair = %{"access" => jwt(3600), "refresh" => "refresh.2"}
    internal = %{"code" => -32_000, "message" => "Internal error", "kind" => "Internal"}
    db = start(%{"refresh" => [{:error, internal}, {:ok, new_pair}]})

    {:ok, _} = SurrealDB.authenticate(db, %{access: jwt(2), refresh: "refresh.1"})
    assert_receive {:rpc, "refresh", [%{"refresh" => "refresh.1"}]}, 3_000
    assert_receive {:rpc, "refresh", [%{"refresh" => "refresh.1"}]}, 3_000
    Process.sleep(50)
    assert %{refresh_token: "refresh.2"} = SurrealDB.info_session(db)
  end

  test "a renewal that cannot be sent is tried again" do
    db = start(%{"signin" => [{:ok, jwt(1)}, {:send_error, :closed}, {:ok, jwt(3600)}]})

    {:ok, _} = SurrealDB.signin(db, %SurrealDB.Auth.Root{user: "root", pass: "root"})
    assert_received {:rpc, "signin", _}
    assert_receive {:rpc, "signin", _}, 2_000
    assert_receive {:rpc, "signin", _}, 3_000
  end

  test "a renewal that times out is tried again" do
    db = start(%{"signin" => [{:ok, jwt(1)}, :noreply, {:ok, jwt(3600)}]}, timeout: 200)

    {:ok, _} = SurrealDB.signin(db, %SurrealDB.Auth.Root{user: "root", pass: "root"})
    assert_received {:rpc, "signin", _}
    assert_receive {:rpc, "signin", _}, 2_000
    assert_receive {:rpc, "signin", _}, 3_000
  end

  test "a renewal that fails after a newer refresh keeps the newer refresh token" do
    pair = %{"access" => jwt(2), "refresh" => "refresh.1"}
    new_pair = %{"access" => jwt(3600), "refresh" => "refresh.2"}
    refused = %{"code" => -32_002, "message" => "Invalid refresh token", "kind" => "NotAllowed"}

    db =
      start(%{
        "signin" => {:ok, pair},
        "refresh" => [{:delay, 400, {:error, refused}}, {:ok, new_pair}]
      })

    {:ok, _} = SurrealDB.signin(db, @record)
    assert_received {:rpc, "signin", _}
    assert_receive {:rpc, "refresh", [^pair]}, 3_000
    assert {:ok, ^new_pair} = SurrealDB.refresh(db)

    refute_receive {:rpc, "signin", _}, 700
    assert %{refresh_token: "refresh.2"} = SurrealDB.info_session(db)
  end

  test "a renewal that succeeds after a newer authenticate does not undo it" do
    db = start(%{"signin" => [{:ok, jwt(2)}, {:delay, 400, jwt(3600, "old")}]})

    {:ok, _} = SurrealDB.signin(db, %SurrealDB.Auth.Root{user: "root", pass: "root"})
    assert_received {:rpc, "signin", _}
    assert_receive {:rpc, "signin", _}, 3_000
    {:ok, _} = SurrealDB.authenticate(db, "newer.token")
    assert_received {:rpc, "authenticate", ["newer.token"]}

    # The renewal's late success is dropped, and the session put back on the newer token.
    assert_receive {:rpc, "authenticate", ["newer.token"]}, 1_000
    assert %{token: "newer.token"} = SurrealDB.info_session(db)
  end

  test "a forked session renews its own access token" do
    pair = %{"access" => jwt(2), "refresh" => "refresh.1"}
    new_pair = %{"access" => jwt(3600), "refresh" => "refresh.2"}
    db = start(%{"signin" => {:ok, pair}, "refresh" => {:ok, new_pair}})

    {:ok, _} = SurrealDB.signin(db, @record)
    {:ok, _session} = SurrealDB.fork_session(db)

    assert_receive {:rpc, "refresh", _}, 3_000
    assert_receive {:rpc, "refresh", _}, 3_000
  end

  test "does not renew with renew_access: false, or with only a token" do
    db = start(%{"signin" => {:ok, jwt(1)}}, renew_access: false)
    {:ok, _} = SurrealDB.signin(db, %SurrealDB.Auth.Root{user: "root", pass: "root"})
    assert_received {:rpc, "signin", _}

    other = start(%{}, [], :other)
    {:ok, _} = SurrealDB.authenticate(other, jwt(1))

    refute_receive {:rpc, "signin", _}, 1_500
    assert :sys.get_state(db).renewals == %{}
    assert :sys.get_state(other).renewals == %{}
  end

  test "a reconnect refreshes when the access token is refused" do
    pair = %{"access" => "access.1", "refresh" => "refresh.1"}
    new_pair = %{"access" => "access.2", "refresh" => "refresh.2"}
    expired = %{"code" => -32_002, "message" => "Token expired", "kind" => "NotAllowed"}

    db =
      start(%{
        "signin" => {:ok, pair},
        "authenticate" => {:error, expired},
        "refresh" => {:ok, new_pair}
      })

    {:ok, _} = SurrealDB.signin(db, @record)
    assert_received {:rpc, "signin", _}
    :ok = SurrealDB.subscribe_status(db)

    send(db, :mock_closed)
    assert_receive {:surrealdb, :status, :connected, _}, 1_000

    assert_received {:rpc, "authenticate", ["access.1"]}
    assert_received {:rpc, "refresh", [^pair]}
    refute_received {:rpc, "signin", _}
    assert %{token: "access.2", refresh_token: "refresh.2"} = SurrealDB.info_session(db)
  end

  test "a reconnect whose refresh fails for another reason keeps the pair and tries again" do
    new_pair = %{"access" => "access.2", "refresh" => "refresh.2"}
    expired = %{"code" => -32_002, "message" => "Token expired", "kind" => "NotAllowed"}
    internal = %{"code" => -32_000, "message" => "Internal error", "kind" => "Internal"}

    db =
      start(%{
        "authenticate" => [{:ok, nil}, {:error, expired}],
        "refresh" => [{:error, internal}, {:ok, new_pair}]
      })

    {:ok, _} = SurrealDB.authenticate(db, %{access: "access.1", refresh: "refresh.1"})
    :ok = SurrealDB.subscribe_status(db)
    send(db, :mock_closed)

    assert_receive {:surrealdb, :status, :reconnecting, %{attempt: 2}}, 2_000
    assert_receive {:surrealdb, :status, :connected, %{attempt: 2}}, 2_000
    assert %{token: "access.2", refresh_token: "refresh.2"} = SurrealDB.info_session(db)
  end

  test "a reconnect whose refresh is refused, with no credentials to sign in with, fails" do
    expired = %{"code" => -32_002, "message" => "Token expired", "kind" => "NotAllowed"}
    refused = %{"code" => -32_002, "message" => "Invalid refresh token", "kind" => "NotAllowed"}

    db =
      start(%{
        "authenticate" => [{:ok, nil}, {:error, expired}, {:ok, nil}],
        "refresh" => {:error, refused}
      })

    {:ok, _} = SurrealDB.authenticate(db, %{access: "access.1", refresh: "refresh.1"})
    :ok = SurrealDB.subscribe_status(db)
    send(db, :mock_closed)

    assert_receive {:surrealdb, :status, :reconnecting, %{attempt: 2}}, 2_000
    assert_receive {:surrealdb, :status, :connected, %{attempt: 2}}, 2_000
    refute_received {:rpc, "signin", _}
    assert %{token: "access.1"} = SurrealDB.info_session(db)
  end

  test "a revoke that times out still ends renewal when its reply comes late" do
    pair = %{"access" => jwt(3600), "refresh" => "refresh.1"}
    db = start(%{"signin" => {:ok, pair}, "revoke" => :noreply}, timeout: 100)

    {:ok, _} = SurrealDB.signin(db, @record)
    assert {:error, %SurrealDB.Error{kind: :timeout}} = SurrealDB.revoke(db)

    [id] = Map.keys(:sys.get_state(db).pending)
    send(db, {:mock_message, SurrealDB.Codec.CBOR.encode(%{"id" => id, "result" => nil})})

    assert %{refresh_token: nil} = SurrealDB.info_session(db)
    assert :sys.get_state(db).sessions[nil].credentials == nil
    assert :sys.get_state(db).renewals == %{}
  end

  test "a failed reassertion after a stale renewal drops the connection to restore it" do
    refused = %{"code" => -32_002, "message" => "Not allowed", "kind" => "NotAllowed"}

    db =
      start(%{
        "signin" => [{:ok, jwt(2)}, {:delay, 400, jwt(3600, "old")}],
        "authenticate" => [{:ok, nil}, {:error, refused}, {:ok, nil}]
      })

    {:ok, _} = SurrealDB.signin(db, %SurrealDB.Auth.Root{user: "root", pass: "root"})
    :ok = SurrealDB.subscribe_status(db)
    assert_received {:rpc, "signin", _}
    assert_receive {:rpc, "signin", _}, 3_000
    {:ok, _} = SurrealDB.authenticate(db, "newer.token")

    assert_receive {:surrealdb, :status, :reconnecting, _}, 2_000
    assert_receive {:surrealdb, :status, :connected, _}, 2_000
    assert %{token: "newer.token"} = SurrealDB.info_session(db)
  end

  test "a reassertion that times out drops the connection to restore it" do
    db =
      start(
        %{
          "signin" => [{:ok, jwt(2)}, {:delay, 400, jwt(3600, "old")}],
          "authenticate" => [{:ok, nil}, :noreply, {:ok, nil}]
        },
        timeout: 1_000
      )

    {:ok, _} = SurrealDB.signin(db, %SurrealDB.Auth.Root{user: "root", pass: "root"})
    :ok = SurrealDB.subscribe_status(db)
    assert_receive {:rpc, "signin", _}
    assert_receive {:rpc, "signin", _}, 3_000
    {:ok, _} = SurrealDB.authenticate(db, "newer.token")

    assert_receive {:surrealdb, :status, :reconnecting, _}, 3_000
    assert_receive {:surrealdb, :status, :connected, _}, 2_000
    assert %{token: "newer.token"} = SurrealDB.info_session(db)
  end

  test "auth that changes while a reassertion is in flight is put back again" do
    db =
      start(%{
        "signin" => [{:ok, jwt(2)}, {:delay, 400, jwt(3600, "old")}],
        "authenticate" => [{:ok, nil}, {:delay, 300, nil}, {:ok, nil}]
      })

    {:ok, _} = SurrealDB.signin(db, %SurrealDB.Auth.Root{user: "root", pass: "root"})
    assert_receive {:rpc, "signin", _}
    assert_receive {:rpc, "signin", _}, 3_000
    {:ok, _} = SurrealDB.authenticate(db, "newer.token")
    assert_received {:rpc, "authenticate", ["newer.token"]}

    # The reassertion goes out, and the auth changes before it is answered.
    assert_receive {:rpc, "authenticate", ["newer.token"]}, 1_000
    {:ok, _} = SurrealDB.authenticate(db, "newest.token")
    assert_received {:rpc, "authenticate", ["newest.token"]}

    assert_receive {:rpc, "authenticate", ["newest.token"]}, 1_000
    assert %{token: "newest.token"} = SurrealDB.info_session(db)
  end

  test "a stale renewal for a detached session is not reasserted" do
    db = start(%{"signin" => [{:ok, jwt(2)}, {:delay, 400, jwt(3600, "old")}]})
    {:ok, session} = SurrealDB.new_session(db)

    {:ok, _} = SurrealDB.signin(session, %SurrealDB.Auth.Root{user: "root", pass: "root"})
    assert_receive {:rpc, "signin", _}
    assert_receive {:rpc, "signin", _}, 3_000
    {:ok, _} = SurrealDB.close_session(session)

    refute_receive {:rpc, "invalidate", _}, 800
    refute_received {:rpc, "authenticate", _}
    assert Map.keys(:sys.get_state(db).sessions) == [nil]
  end
end
