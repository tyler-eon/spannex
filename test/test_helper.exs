defmodule TestLifecycle do
  @project "projects/test"
  @instance "#{@project}/instances/test"
  @database "#{@instance}/databases/test"

  def connect(host, opts \\ []) do
    headers =
      opts
      |> Keyword.get(:headers, [])
      |> List.keystore("content-type", 0, {"content-type", "application/grpc"})

    GRPC.Stub.connect(host, Keyword.put(opts, :headers, headers))
  end

  def setup(channel) do
    request = %Google.Spanner.Admin.Instance.V1.CreateInstanceRequest{
      parent: @project,
      instance_id: "test",
      instance: %Google.Spanner.Admin.Instance.V1.Instance{
        name: @instance,
        config: "#{@project}/instanceConfigs/emulator-config",
        display_name: "test"
      }
    }

    {:ok, _} = Google.Spanner.Admin.Instance.V1.InstanceAdmin.Stub.create_instance(channel, request)

    request = %Google.Spanner.Admin.Database.V1.CreateDatabaseRequest{
      parent: @instance,
      create_statement: "CREATE DATABASE test",
      extra_statements: [
        "CREATE TABLE foo (id STRING(36) DEFAULT(GENERATE_UUID()), str STRING(100), num INT64, flo FLOAT64) PRIMARY KEY (id)",
        "CREATE TABLE bar (id STRING(36) DEFAULT(GENERATE_UUID()), foo_id STRING(36) NOT NULL, notnullstr STRING(100) NOT NULL, notnullnum INT64 NOT NULL) PRIMARY KEY (id)",
        "CREATE INDEX bar_by_foo ON bar (foo_id)"
      ]
    }

    {:ok, _} = Google.Spanner.Admin.Database.V1.DatabaseAdmin.Stub.create_database(channel, request)

    {:ok, @database}
  end

  def teardown(channel) do
    request = %Google.Spanner.Admin.Database.V1.DropDatabaseRequest{
      database: @database
    }

    {:ok, _} = Google.Spanner.Admin.Database.V1.DatabaseAdmin.Stub.drop_database(channel, request)

    request = %Google.Spanner.Admin.Instance.V1.DeleteInstanceRequest{
      name: @instance
    }

    {:ok, _} = Google.Spanner.Admin.Instance.V1.InstanceAdmin.Stub.delete_instance(channel, request)

    GRPC.Stub.disconnect(channel)
  end
end

{:ok, _} = Goth.start_link(name: Spannex.Goth)

# If we have `SPANNER_EMULATOR_HOST` set, use that for local testing.
opts =
  case System.get_env("SPANNER_EMULATOR_HOST") do
    nil ->
      # Otherwise we *require* `SPANNER_DATABASE` to be set and Goth to be configured.
      opts = [name: Spannex.Conn, database: System.fetch_env!("SPANNER_DATABASE"), goth: Spannex.Goth]

      case System.get_env("SPANNER_HOST") do
        nil ->
          opts

        host ->
          [{:host, host} | opts]
      end

    host ->
      # Because we're using the emulator, we need to create a new "instance" and a new database for that instance.
      {:ok, channel} = TestLifecycle.connect(host)
      {:ok, database} = TestLifecycle.setup(channel)

      ExUnit.after_suite(fn _ ->
        TestLifecycle.teardown(channel)
      end)

      [name: Spannex.Conn, host: host, database: database, goth: Spannex.Goth]
  end

ExUnit.start()

{:ok, _} = Spannex.start_link(opts)

defmodule Spannex.TestHelper do
end
