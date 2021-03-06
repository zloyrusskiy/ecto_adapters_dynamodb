use Mix.Config

config :ecto_adapters_dynamodb, Ecto.Adapters.DynamoDB.TestRepo,
  adapter: Ecto.Adapters.DynamoDB,
  migration_source: "test_schema_migrations", # When running migrations during testing, use this table to track migration history
  # ExAws configuration
  debug_requests: true,
  # Unlike for prod config, we hardcode fake values for local version of DynamoDB
  access_key_id: "abcd",
  secret_access_key: "1234",
  region: "us-east-1",
  dynamodb: [
    scheme: "http://",
    host: "localhost",
    port: 8000,
    region: "us-east-1"
  ]

config :ecto_adapters_dynamodb,
  dynamodb_local: true,
  log_levels: [],
  scan_tables: ["test_schema_migrations"]

config :logger,
  backends: [:console],
  compile_time_purge_level: :debug,
  level: :info
