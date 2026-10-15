# Integration tests need a running SurrealDB server and are excluded by default.
# Run them with: mix test --include integration
#
# Reconnect tests wait on real timers, so give assert_receive more than the default 100ms
# for when the suite runs many tests in parallel.
ExUnit.start(exclude: [:integration], assert_receive_timeout: 1_000)
