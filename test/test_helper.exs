# Integration tests need a running SurrealDB server and are excluded by default.
# Run them with: mix test --include integration
ExUnit.start(exclude: [:integration])
