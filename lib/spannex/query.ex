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
  alias Spannex.Typing

  def parse(query, _opts), do: query

  def describe(query, _opts), do: query

  @doc """
  Returns the encoded parameters for the query. Parameters are expected to be given as a map of `{name, value}` pairs.

  Because queries are executed using protobuf messages, the parameters are encoded into native protobuf types such as `Google.Protobuf.Value`.

  Some parameters might look odd in their encoded state due to some restrictions on how protobuf messages are encoded and decoded, specifically restrictions inherent in the official JSON specification. For example, 64-bit integers are not officially supported by JSON, so we have to encode them as strings.
  """
  def encode(_query, params, _opts) when is_map(params), do: Typing.encode(params)

  @doc """
  Returns an Elixir-native representation of the result set.
  """
  def decode(_query, %Google.Spanner.V1.ResultSet{metadata: %{row_type: %{fields: fields}}, rows: rows}, _opts) do
    Enum.map(rows, fn %{values: row} ->
      decode_row(fields, row, %{})
    end)
  end

  defp decode_row([], [], acc), do: acc

  defp decode_row([%{name: field, type: %{code: type}} | fields], [%{kind: {_, wire_value}} | rows], acc) do
    acc = Map.put(acc, field, Typing.decode(type, wire_value))
    decode_row(fields, rows, acc)
  end
end
