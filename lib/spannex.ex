defmodule Spannex do
  @moduledoc """
  The entry module for interacting with `Spannex.Protocol`, the underlying `DBConnection` implementation.

  To create a Spannex database connection to Cloud Spanner, simply call `Spannex.start_link/1` with the database configuration or pass in `{Spannex, opts}` to your supervisor.

  ## Queries vs Mutations

  Spanner makes a clear distinction between "queries", which are read-only statements, and "mutations", which are statements that modify the state of the database. However, Spannex still provides a single function, `execute/4`, and will perform any necessary internal conversions depending on the type of the statement being executed.

  ## Named Variables

  Currently, only named variables are accepted in queries and mutations. You must also pass in a map of named variables when executing a statement that contains named variables. For example, if you have a query like `SELECT * FROM users WHERE id = @id`, you must pass in a map containing `%{id: 1}` when executing the statement.
  """

  alias Spannex.Query

  @doc """
  Returns a child specification for the Spannex database connection using the given options.

  You *must* supply at least one option, `:database`, to specify the Cloud Spanner database to connect to.

  It is recommended to avoid supplying the `:name` option and to instead let the connection pool handle fetching and returning individual connections.

  See `Spannex.Protocol.connect/1` for a full list of accepted options.
  """
  def child_spec(opts), do: DBConnection.child_spec(Spannex.Protocol, opts)

  @doc """
  Starts a Spannex database connection using the given options.

  Synonymous with `DBConnection.start_link(Spannex.Protocol, opts)`.

  You *must* supply at least one option, `:database`, to specify the Cloud Spanner database to connect to.

  See `Spannex.Protocol.connect/1` for a full list of accepted options.
  """
  def start_link(opts), do: DBConnection.start_link(Spannex.Protocol, opts)

  @doc """
  Runs the given function inside a Spanner transaction.
  """
  def transaction(conn, fun, opts \\ []), do: DBConnection.transaction(conn, fun, opts)

  @doc """
  Converts a string SQL statement into a `Spannex.Query` struct before calling `DBConnection.prepare_execute/4`.
  """
  def execute(conn, statement, params, opts \\ []) do
    DBConnection.prepare_execute(conn, %Query{statement: statement}, params, opts)
  end
end
