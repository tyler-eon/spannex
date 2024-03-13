defmodule Spannex.Protocol do
  @moduledoc """
  A `DBConnection` implementation for Cloud Spanner.

  ## Connections

  Each connection maintains its own gRPC channel to the Spanner API and its own unique session. The session is used as a unique identifier for all other requests made against the database.

  ## Transactions

  When starting a transaction, there are four accepted types of transactions:

  1. `:batch_write` - Uses "batch write" and "mutation groups" to execute multiple operations in a single transaction, optimized for "blind bulk writes", low-latency, and atomicity within each group of mutations.
  2. `:read_write` - A locking read-write transaction, this is what you would consider a "traditional transaction". It's best used when you are changing one row or a handful of related rows or need to reference data created in the same transaction.
  3. `:read_only` - A special transaction type that allows for point-in-time consistency in the data being read. This helps eliminate possible drift in values when operations might be happening in parallel to the read-only transaction by locking the state of the data being read to a specific point in time.
  4. `:partitioned_dml` - A special transaction where regular DML statements are partitioned and executed in parallel. This is useful for large-scale bulk operations that still need guarantees of atomicity and consistency.

  The `:batch_write` transaction differs from `:read_write` and `:partitioned_dml` transactions in some important ways:

  1. It does not support reading data. It is strictly for writing data.
  2. You cannot reference changes made by other mutations within the same transaction.
  3. You must specify all literals explicitly, you cannot use named query parameters.
  4. All queries are locally queued and then sent all at once when the transaction is committed. This means individual statements are not sent to the database, rather they are batched into a single gRPC request when the transaction is comitted.
  5. Deletions *must* be done by primary key. If you need a custom where-clause to delete rows, you must do so in a `:read_write` or `:partitioned_dml` transaction using regular DML statements.

  ### Timeouts

  Transactions have a 10-second idle timeout that cannot be modified. If a transaction remains idle for more than 10 seconds, it is aborted. Aborted transactions cause the session to enter a limbo state where you can't really do anything useful anymore, so the default behavior is to disconnect and force the creation of a new connection.

  With the exception of a `:batch_write` transactions, all individual statements resolved via `handle_execute` will reset the 10-second idle timer. But if you start a `:batch_write` transaction, you have 10 seconds to queue up all necessary statements and then commit the transaction. While this should be sufficient in most scenarios, it is worth keeping this in mind if you are using `:batch_write` transactions for very large operations. In such cases where the 10-second idle timeout might be a problem, consider pre-calculating as much data for the DML statements as possible and then starting a `:batch_write` transaction after.

  ## Queries

  Spanner requires that all write operations, i.e. queries that are not `SELECT` statements, use an explicit transaction. If you attempt to execute a write statement outside of a transaction, you will receive an error.

  For read queries, i.e. `SELECT` statements, a temporary read-only transaction with a strong consistency guarantee can be created for you.

  Spanner also stores version history for all columns and all rows and all tables, meaning you can get truly strong consistency guarantees when reading data. You can ensure that, given a specific (recent) point in time, all data read from the database is no more recent than that point in time.
  """

  use DBConnection

  require Logger

  alias Google.Spanner.V1, as: Spanner
  alias Google.Spanner.V1.Spanner.Stub, as: Service

  @type transaction_type :: :none | :batch_write | :read_write | :read_only | :partitioned_dml

  @type t :: %__MODULE__{
    channel: GRPC.Channel.t(),
    session: Spanner.Session.t(),
    status: DBConnection.status(),
    transaction: transaction_type(),
    transaction_id: String.t(),
    seqno: non_neg_integer(),
    queries: [DBConnection.query()],
  }

  defstruct [
    channel: nil,
    session: nil,
    status: :idle,
    transaction: :none,
    transaction_id: nil,
    seqno: 0,
    queries: [],
  ]

  @doc """
  Start a new gRPC connection to the Spanner API and also generate a new session.

  There is only one required option for the `connect/1` function: `:database`. This tells Spannex which database the session will be for.

  If you do not supply an authorization header as part of `:grpc_opts`, you must also supply the `:goth` option, which tells Spannex how to fetch an access token.

  You may optionally supply the following other options:

  - `:goth` - The name of the Goth instance used to fetch access tokens. Required if no authorization header is supplied as part of `:grpc_opts`.
  - `:labels` - A map of labels to apply to the session. These labels can be used to filter and monitor sessions using the Cloud console or admin API.
  - `:host` - The gRPC endpoint to connect to. If not supplied, the global Spanner API endpoint is used.
  - `:grpc_opts` - The options to pass to `GRPC.Stub.connect/2`.
  - `:cred` - A gRPC credential wrapper. If not supplied, the `Goth` library is used to fetch a Google Cloud access token.
  - `:headers` - A list of headers to use for the connection. If not supplied, `content-type` and `authorization` headers will be set to sane defaults.
  """
  @impl DBConnection
  def connect(opts) do
    # Allows DBConnection to disconnect on unexpected exits.
    Process.flag(:trap_exit, true)

    case grpc_connect(opts) do
      {:ok, channel} ->
        create_session(channel, opts)

      error ->
        error
    end
  end

  # Connect to the Spanner API endpoint over a gRPC channel.
  defp grpc_connect(opts) do
    grpc_host = Keyword.get(opts, :host, "spanner.googleapis.com:443")
    grpc_opts = Keyword.get(opts, :grpc_opts, [])

    # We require `content-type` and `authorization` headers to be set during the gRPC connection.
    headers = Keyword.get(grpc_opts, :headers, [])

    # If no authorization header is supplied, use goth to fetch an access token.
    headers =
      if List.keymember?(headers, "authorization", 0) do
        headers
      else
        %{
          type: type,
          token: token
        } = Goth.fetch!(Credits.Goth)
        [{"authorization", "#{type} #{token}"} | headers]
      end

    # Spanner requires content-type to be set to "application/grpc".
    headers = List.keystore(headers, "content-type", 0, {"content-type", "application/grpc"})

    # If `cred` is not set, but `443` is specified in the host, add a default SSL-enabled `cred` option.
    grpc_opts =
      case Keyword.get(opts, :cred, nil) do
        nil ->
          if String.ends_with?(grpc_host, ":443") do
            # TODO: Should probably default to using the Erlang SSL library to specify cacerts by default.
            Keyword.put(grpc_opts, :cred, GRPC.Credential.new(ssl: [verify: :verify_none]))
          else
            grpc_opts
          end

        _ ->
          grpc_opts
      end

    grpc_opts = Keyword.put(grpc_opts, :headers, headers)

    Logger.debug("Connecting to the Spanner gRPC endpoint: #{grpc_host}", host: grpc_host, opts: opts)
    GRPC.Stub.connect(grpc_host, grpc_opts)
  end

  # Given a gRPC channel, create a new session for the database connection.
  defp create_session(channel, opts) do
    database = Keyword.fetch!(opts, :database)
    labels = Keyword.get(opts, :labels, %{})

    request = %Spanner.CreateSessionRequest{
      database: database,
      session: %Spanner.Session{
        labels: labels
      }
    }

    Logger.debug("Creating a new Spanner session to #{database}", database: database, labels: labels)
    case Service.create_session(channel, request) do
      {:ok, session} ->
        {:ok, %__MODULE__{channel: channel, session: session}}

      error ->
        error
    end
  end

  @impl DBConnection
  def disconnect(_err, %{channel: channel, session: session}) do
    Logger.debug("Disconnecting from Spanner", session: session)
    Service.delete_session(
      channel,
      %Spanner.DeleteSessionRequest{name: session.name}
    )
    GRPC.Stub.disconnect(channel)
    :ok
  end

  @doc """
  No-op
  """
  @impl DBConnection
  def checkout(state), do: {:ok, state}

  @doc """
  No-op
  """
  @impl DBConnection
  def ping(state), do: {:ok, state}

  @doc """
  Begins a new transaction. You *must* use a transaction for any mutations, i.e. anything that isn't a `SELECT` query.

  May specify the option `:type` as any one of: `:batch_write`, `:read_write`, `:read_only`, `:partitioned_dml`. If `:type` is not set, it defaults to `:read_write`.

  If `:batch_write` is specified, all subsequent `handle_execute/4` calls will simply queue up the statements locally in a list that will be converted to a set of mutations when `handle_commit/2` is called. If `handle_rollback/2` is called instead, the queue is simply cleared, nothing happens remotely.
  """
  @impl DBConnection
  def handle_begin(opts, %{status: :idle} = state) do
    case Keyword.get(opts, :type, :read_write) do
      :batch_write ->
        {:ok, %{state | status: :transaction, transaction: :batch_write}}

      type ->
        mode =
          case type do
            :read_write ->
              %Spanner.TransactionOptions.ReadWrite{}

            :read_only ->
              %Spanner.TransactionOptions.ReadOnly{
                return_read_timestamp: true
              }

            :partitioned_dml ->
              %Spanner.TransactionOptions.PartitionedDml{}
          end

        request = %Spanner.BeginTransactionRequest{
          session: state.session.name,
          options: %Spanner.TransactionOptions{
            mode: {type, mode}
          }
        }

        case Service.begin_transaction(state.channel, request) do
          {:ok, %{id: tx_id}} ->
            {:ok, nil, %{state | status: :transaction, transaction: type, transaction_id: tx_id, seqno: 0}}

          {:error, error} ->
            {:error, error, state}
        end
    end
  end

  def handle_begin(_opts, %{status: status} = state), do: {status, state}

  @doc """
  Commits a running transaction.

  If the transaction is a `:batch_write` transaction, all queued statements are converted to "mutations" and sent to the Spanner API in a single request.
  """
  @impl DBConnection
  def handle_commit(_opts, %{status: :transaction, transaction: :batch_write} = state) do
    # Don't have a great way to separate groups of statements into multiple mutations groups,
    # so for now we just always commit a single mutation group filled with all the pending queries.
    mutations = Enum.map(state.queries, &(query_to_mutation(&1)))
    request = %Spanner.BatchWriteRequest{
      session: state.session.name,
      mutation_groups: [
        %Spanner.BatchWriteRequest.MutationGroup{
          mutations: mutations
        }
      ]
    }
    {:ok, %Spanner.BatchWriteResponse{status: status}} = Service.batch_write(state.channel, request)
    case status do
      %{code: 0} ->
        {:ok, :ok, %{state | status: :idle, transaction: :none, queries: []}}

      error ->
        {:disconnect, error, state}
    end
  end

  def handle_commit(_opts, %{status: :transaction} = state) do
    request = %Spanner.CommitRequest{
      session: state.session.name,
      transaction: {:transaction_id, state.transaction_id},
      return_commit_stats: true
    }
    case Service.commit(state.channel, request) do
      {:ok, %Spanner.CommitResponse{commit_stats: stats}} ->
        {:ok, stats, %{state | status: :idle, transaction: :none, transaction_id: nil}}

      {:error, error} ->
        {:disconnect, error, state}
    end
  end

  def handle_commit(_opts, %{status: status} = state), do: {status, state}

  @doc """
  Rolls back a running transaction.

  If the transaction is a `:batch_write` transaction, all queued statements are simply cleared and the state is returned to idle.
  """
  @impl DBConnection
  def handle_rollback(_opts, %{status: :transaction, transaction: :batch_write} = state) do
    # Since batch write transaction queue up statements locally, we can just clear the queue and return to idle.
    {:ok, :ok, %{state | status: :idle, transaction: :none, queries: []}}
  end

  def handle_rollback(_opts, %{status: :transaction} = state) do
    request = %Spanner.RollbackRequest{
      session: state.session.name,
      transaction_id: state.transaction_id
    }
    case Service.rollback(state.channel, request) do
      {:ok, _} ->
        {:ok, :ok, %{state | status: :idle, transaction: :none, transaction_id: nil}}

      {:error, error} ->
        {:disconnect, error, state}
    end
  end

  def handle_rollback(_opts, %{status: status} = state), do: {status, state}

  @doc """
  No-op
  """
  @impl DBConnection
  def handle_close(_query, _opts, state), do: {:ok, nil, state}

  @doc """
  Executes a SQL statement against the Spanner API.

  All statements that are mutations (i.e. not `SELECT` statements) must be executed within a transaction or they will produce an error.

  If a `SELECT` statement is executed outside of a transaction, the database will generate a temporary `:read_only` transaction to run it in.

  If the transaction type is `:batch_write`, the query will be queued up locally and not sent to the Spanner API until `handle_commit/2` is called. This means the result portion of the response tuple will be `nil` since there's no result to return yet.
  """
  @impl DBConnection
  def handle_execute(query, params, _opts, %{status: :transaction, transaction: :batch_write} = state) do
    {:ok, query, nil, %{state | queries: [%{query | params: params} | state.queries]}}
  end

  def handle_execute(query, params, _opts, %{status: :transaction, seqno: seqno} = state) do
    # If we're in a transaction, we're guaranteed that we can execute the statement.
    request = %Spanner.ExecuteSqlRequest{
      session: state.session.name,
      transaction: %Spanner.TransactionSelector{
        selector: {:id, state.transaction_id}
      },
      sql: query.statement,
      params: wrap_params(params),
      seqno: seqno,
    }
    case Service.execute_sql(state.channel, request) do
      {:ok, %Spanner.ResultSet{} = results} ->
        {:ok, query, decode_results(results), %{state | seqno: seqno + 1}}

      {:error, error} ->
        {:disconnect, error, state}
    end
  end

  def handle_execute(%{statement: "SELECT" <> _} = query, params, _opts, state) do
    # A select request *does not* require an explicit transaction, it will generate a temporary read-only transaction for us.
    request = %Spanner.ExecuteSqlRequest{
      session: state.session.name,
      sql: query.statement,
      params: wrap_params(params)
    }
    {:ok, %Spanner.ResultSet{} = results} = Service.execute_sql(state.channel, request)
    {:ok, query, decode_results(results), state}
  end

  def handle_execute(_query, _params, _opts, state) do
    # All requests that are not SELECTs require an explicit transaction.
    {:error, %GRPC.RPCError{status: 9, message: "Cannot execute write statements outside of a transaction."}, state}
  end

  # Wrap a set of named parameters in a `Google.Protobuf.Struct`.
  defp wrap_params(%{params: params}) do
    fields =
      params
      |> Enum.map(fn
        {key, value} when is_atom(key) ->
          {Atom.to_string(key), wrap_value(value)}
        {key, value} when is_binary(key) ->
          {key, wrap_value(value)}
      end)
      |> Enum.into(%{})
    %Google.Protobuf.Struct{fields: fields}
  end

  # Special nil value.
  defp wrap_value(nil), do: %Google.Protobuf.Value{kind: {:null_value, :NULL_VALUE}}

  # Whoa wtf is this? Well, gRPC encodes to JSON, but integers in JSON only are guaranteed up to 32-bits. So how does Google solve this?
  # They use strings! Any INT64 data type is returned as a string and must be sent as a string to be converted back to a numeric value at the destination.
  defp wrap_value(value) when is_integer(value), do: %Google.Protobuf.Value{kind: {:string_value, Integer.to_string(value)}}

  # Standard types.
  defp wrap_value(value) when is_binary(value), do: %Google.Protobuf.Value{kind: {:string_value, value}}
  defp wrap_value(value) when is_float(value), do: %Google.Protobuf.Value{kind: {:number_value, value}}
  defp wrap_value(value) when is_boolean(value), do: %Google.Protobuf.Value{kind: {:bool_value, value}}

  # Aggregate types.
  #defp wrap_value(value) when is_list(value), do: %Google.Protobuf.Value{kind: {:list_value, wrap_list(value)}}
  #defp wrap_value(value) when is_map(value), do: %Google.Protobuf.Value{kind: {:struct_value, wrap_struct(value)}}

  @doc """
  No-op
  """
  @impl DBConnection
  def handle_prepare(query, _opts, state), do: {:ok, query, state}

  @doc """
  Would normally generate a cursor for a query prepared by `handle_prepare/3`, but we don't support cursors in this implementation.
  """
  @impl DBConnection
  def handle_declare(query, _params, _opts, state), do: {:ok, query, nil, state}

  @doc """
  Would normally deallocate a cursor generated by `handle_declare/4`, but we don't support cursors in this implementation.
  """
  @impl DBConnection
  def handle_deallocate(_query, _cursor, _opts, state), do: {:ok, nil, state}

  @doc """
  Would normally fetch the next result from a cursor generated by `handle_declare/4`, but we don't support cursors in this implementation.
  """
  @impl DBConnection
  def handle_fetch(_query, _cursor, _opts, state), do: {:halt, nil, state}

  @impl DBConnection
  def handle_status(_opts, %{status: status} = state), do: {status, state}

  # A set of regex patterns that convert GoogleSQL DML into components parts used to make mutations.
  @insert_re ~r/INSERT\s+(?:OR\s+IGNORE|UPDATE)?\s*(?:INTO)?\s+(?<table>\w+)\s*\((?<columns>.*?)\)\s*(?<values>VALUES\s*\((.*?)\)\s*(?:,\s*\(.*?)?)?\s*(?<select>SELECT.*?)/i
  @values_re ~r/VALUES\s*\((.*?)\)(?:\s*,\s*\((.*?)\))*/i
  @update_re ~r/UPDATE\s+(?<table>\w+)\s*(.*?)\s*SET\s+(?<update>.*?)\s*WHERE\s+(?<where>.*?)/i
  @delete_re ~r/DELETE\s+(?:FROM\s+)?(?<table>\w+)\s*(?:.*?WHERE\s+(?<where>.*?))?\s*/i
  @value_re ~r/\w+\s*=\s*(?:'[^']*'|[^,]+)/
  @where_re ~r/\w+\s*(?:[<>!=]+\s*(?:'[^']*'|[^' ]+))?(?=\s*AND|\z)/i

  @doc """
  Parses a SQL query with optional named parameters and converts the operation to a mutation using `make_mutation/4`.
  """
  def query_to_mutation(%{statement: "INSERT " <> _ = sql, params: params}) do
    case Regex.named_captures(@insert_re, sql) do
      %{"table" => table, "columns" => columns, "values" => values_tuples} ->
        columns = split_list(columns)
        values =
          values_tuples
          |> String.split(@values_re)
          |> Enum.map(fn values_str ->
            values_str
            |> split_list()
            |> Enum.map(fn value ->
              replace_with_param(value, params)
            end)
          end)
        {:ok, make_mutation(:insert, table, columns, values)}
      _ ->
        {:error, %GRPC.RPCError{status: 3, message: "Invalid INSERT statement."}}
    end
  end

  def query_to_mutation(%{statement: "UPDATE " <> _ = sql, params: params}) do
    case Regex.named_captures(@update_re, sql) do
      %{"table" => table, "update" => update, "where" => where} ->
        # First parse out the where clause to get columns+values that match primary key identification.
        # Then append the columns+values for the stuff that needs updating.
        # This means UPDATE mutations cannot update primary key columns.
        {columns, values} =
          Regex.scan(@where_re, where) ++ Regex.scan(@value_re, update)
          |> Enum.reduce({[], []}, fn [c, v], {cols, vals} ->
            {[c | cols], [replace_with_param(v, params) | vals]}
          end)

        {:ok, make_mutation(:update, table, columns, values)}
      _ ->
        {:error, %GRPC.RPCError{status: 3, message: "Invalid UPDATE statement."}}
    end
  end

  def query_to_mutation(%{statement: "DELETE " <> _ = sql, params: params}) do
    case Regex.named_captures(@delete_re, sql) do
      %{"table" => table, "where" => where} ->
        values = Enum.map(Regex.scan(@where_re, where), fn [value_str] ->
          [_, value] = split_pair(value_str)
          replace_with_param(value, params)
        end)
        {:ok, make_mutation(:delete, table, nil, values)}
      _ ->
        {:error, %GRPC.RPCError{status: 3, message: "Invalid DELETE statement."}}
    end
  end

  # A list is a series of values delimited by commas.
  defp split_list(str), do: str |> String.split(",") |> Enum.map(&String.trim/1)

  # A pair is a key-value pair separated by the assignment operator.
  defp split_pair(str), do: str |> String.split("=") |> Enum.map(&String.trim/1)

  # If we start with an @ symbol, it's a named parameter, match on the name minus the @ symbol.
  defp replace_with_param("@" <> name, params), do: Map.fetch!(params, name)

  # If we have a string literal (single-quoted value), return the inside value only.
  defp replace_with_param("'" <> value, _params), do: String.slice(value, 1..-2)

  # If it's not a named parameter or a string literal, just return the value as-is.
  defp replace_with_param(value, _params), do: value


  @doc """
  Creates a mutation struct for the given operation. For anything other than a `:delete` operation, the `columns` and `rows` parameters are used to create a `Google.Spanner.V1.Mutation.Write` struct.

  For a `:delete` operation, the Spanner API expects that the primary key will be used to execute the delete mutation. If you need to delete using a where-clause and not the primary key, you cannot use a mutation. The `rows` parameter is expected to be a list of values that correspond to the primary key columns in the same order as they are defined in the table schema.
  """
  def make_mutation(:delete, table, _columns, rows) do
    key_set = %Spanner.KeySet{keys: rows}
    %Spanner.Mutation{
      operation: {:delete, %Spanner.Mutation.Delete{table: table, key_set: key_set}}
    }
  end

  def make_mutation(operation, table, columns, rows) do
    %Spanner.Mutation{
      operation: {
        operation,
        %Spanner.Mutation.Write{table: table, columns: columns, values: rows}
      }
    }
  end

  @doc """
  Converts the row data from a `Google.Spanner.V1.ResultSet` into a list of maps, where each map is one row of the result set. The keys of the map are the column names and the values are the column values. The column values are converted from their encoded form to their Elixir-native representation.
  """
  def decode_results(%Spanner.ResultSet{metadata: %{row_type: %{fields: fields}}, rows: rows}) do
    Enum.map(rows, fn %{values: row} ->
      decode_row(fields, row, %{})
    end)
  end

  def decode_row([], [], acc), do: acc

  def decode_row([%{name: field, type: %{code: type}} | fields], [%{kind: {wire_type, wire_value}} | rows], acc) do
    acc = Map.put(acc, field, convert_value(type, wire_type, wire_value))
    decode_row(fields, rows, acc)
  end

  @doc """
  Converts a single column value from its gRPC-encoded form to its Elixir-native representation. The `type` parameter is the native type of the column and the `enc_type` parameter is the type of the encoded value.
  """
  def convert_value(_, :null_value, :NULL_VALUE), do: nil
  def convert_value(:INT64, :string_value, value), do: String.to_integer(value)
  def convert_value(:FLOAT64, :string_value, value), do: String.to_float(value)
  def convert_value(:BOOL, :string_value, "true"), do: true
  def convert_value(:BOOL, :string_value, "false"), do: false
  def convert_value(:STRING, :string_value, value), do: value
  def convert_value(:BYTES, :string_value, value), do: Base.decode64(value)
  def convert_value(:TIMESTAMP, :string_value, value), do: DateTime.from_iso8601(value)
  def convert_value(:DATE, :string_value, value), do: Date.from_iso8601(value)
  def convert_value(_, _, value), do: value
end
