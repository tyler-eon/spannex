defmodule Spannex.Service do
  @moduledoc """
  A wrapper around the gRPC/Protobuf service stubs.

  Does not necessarily support the entire set of stubbed RPC functions. Will add per-call metadata to the requests as needed.

  **Note**: In the future this module should use `:telemetry` to emit spans for each gRPC request. This would allow applications to understand how long various requests take and also track things like the number of errors and whether they correlate to particular sessions or request types.
  """

  alias Google.Spanner.V1, as: Spanner

  @doc """
  Executes a Spanner gRPC request. Per-request metadata may also be added to the gRPC request.
  """
  @spec execute(Spannex.Protocol.t(), term()) :: {:ok, term()} | {:error, term()}
  # Sessions
  def execute(conn, %Spanner.CreateSessionRequest{} = request), do: call(:create_session, conn, request)
  def execute(conn, %Spanner.DeleteSessionRequest{} = request), do: call(:delete_session, conn, request)

  # Transactions
  def execute(conn, %Spanner.BeginTransactionRequest{} = request), do: call(:begin_transaction, conn, request)
  def execute(conn, %Spanner.CommitRequest{} = request), do: call(:commit, conn, request)
  def execute(conn, %Spanner.RollbackRequest{} = request), do: call(:rollback, conn, request)

  # Queries/SQL/DML
  def execute(conn, %Spanner.ExecuteSqlRequest{} = request), do: call(:execute_sql, conn, request)

  # Mutations
  def execute(conn, %Spanner.BatchWriteRequest{} = request), do: call(:batch_write, conn, request)

  # Generates metadata that should be added to each gRPC request.
  defp request_metadata(state) do
    %{type: type, token: token} = Goth.fetch!(state.goth)

    %{
      "authorization" => type <> " " <> token
    }
  end

  # Invoke the service function with the required arguments.
  defp call(fun, conn, request) do
    apply(Google.Spanner.V1.Spanner.Stub, fun, [conn.channel, request, [metadata: request_metadata(conn)]])
  end
end
