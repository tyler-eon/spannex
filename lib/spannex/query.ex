defmodule Spannex.Query do
  @moduledoc """
  Handles parsing and preparing queries for use by the Spanner database connection protocol.
  """

  defstruct statement: nil,
            params: nil

  @type t :: %__MODULE__{
          statement: String.t(),
          params: Map.t()
        }
end

defimpl DBConnection.Query, for: Spannex.Query do
  def parse(query, _opts), do: query

  def describe(query, _opts), do: query

  def encode(query, params, _opts), do: %{query | params: params}

  def decode(_query, res, _opts), do: res
end
