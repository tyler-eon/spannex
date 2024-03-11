# Spannex

Elixir client for the Google Cloud Spanner database using gRPC.

## Why?

There's really only one option to use Google Cloud Spanner with Elixir right now, and that's the official Google Cloud API version that hasn't been updated in 2 years and uses REST rather than gRPC. What's more, it makes it very difficult to build an Ecto Adapter on top of.

This library instead chooses to implement a Spanner API via [DBConnection](https://hexdocs.pm/db_connection/DBConnection.html) and uses gRPC as its connection mechanism.

At the moment, each individual connection generates its own "channel" rather than sharing one. This is technically not necessary, as Spanner uses *sessions* to determine "who" is performing which operations, not the gRPC socket channel. But the complexity of generating fewer channels and sharing them is a bit more than needed at this time, and it can be an improvement down the road if needed.

## Getting Started

Add the library to your project:

```elixir
defp dependencies() do
  {:spannex, "~> 0.1"}
end
```

Then you just start it like you would any other `DBConnection`: using `start_link/2` manually or by passing a child spec to a supervisor. The latter is probably more common, and it is recommended to use `DBConnection.child_spec` to create an appropriate child spec.

```elixir
DBConnection.child_spec(Spannex.Protocol, [
  name: MyApp.Spanner,
  pool_size: 5,
  database: "projects/p/instances/i/databases/d",
])
```

The only *required* option is `:database`. Notice how the example above uses the fully-qualified database name. If you don't set this, or don't use the appropriate format like is shown above, you'll get an error. Not setting it will result in a runtime error where an invalid database name will result in an error tuple.

By default, `Spannex` will use the global Spanner API endpoint for new connections: `spanner.googleapis.com`. If you need to ensure connections are regional for whatever reason, you can manually override that in your child spec by setting `:host` in the options.

Additionally, `Spannex` uses [goth](https://github.com/peburrows/goth) to fetch the appropriate Google Cloud Credentials to authenticate when opening a gRPC channel to the API. Ensure that whatever user or service account credentials you use have the appropriate permissions to interact with the Spanner database.

*Note*: the Spanner API must be enabled in the project your application is running in, even if the Spanner instance itself is in another project. Failing to have the Spanner API enabled in the application project will result in a runtime error.

And because we use [grpc](https://github.com/elixir-grpc/grpc) to communicate with the database, you can also fine-tune you gRPC configuration by setting `:grpc_opts` in the child spec options.

## Querying

Right now only "simple" querying is supported. The query wrapper, `Spannex.Query`, only has two fields: `:statement` and `:params`. When a statement is executed immediately, i.e. in a `:read_write` transaction during a `handle_execute/4` callback, the supplied parameters completely override the encoded `Spannex.Query` parameters. In fact, the encoded query parameters are only ever used during `:batch_write` transaction commits, since all of the SQL statements for batch write mutations are queued locally and "executed" only during the commit phase.

## Protobuf

The repo contains a "pinned" version of the necessary Protobuf files required to successfully run `Spannex`.

While it is possible to build the protobuf output dynamically, they don't change often. And even if they did change, we would want to pin the version that works for us until we could upgrade safely. Therefore it makes sense to do the generation once and then save the output to the repo for easier/faster application building.

There might actually be *more* protobuf files generated than are actually the minimum set necessary to run this project. The list might be trimmed down over time to avoid unnecessary code in the repo, but right now I basically just generated `*.pb` to ensure I had everything I would to get this thing working properly. And the admin protobuf files are useful because if someone uses `Spannex` for their project, they might also have a need for the admin API, and so they could set up their own gRPC connection and already have all of the admin protobuf files at the ready.
