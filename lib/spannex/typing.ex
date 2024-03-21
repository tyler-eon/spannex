defmodule Spannex.Typing do
  @moduledoc """
  Provides typing conversions between Elixir-native types and protobuf types.
  """

  @doc """
  Encodes an Elixir-native term into a protobuf "type wrapper."
  """
  @spec encode(term) :: Google.Protobuf.Value.t | Google.Protobuf.ListValue.t | Google.Protobuf.Struct.t
  def encode(nil), do: %Google.Protobuf.Value{kind: {:null_value, :NULL_VALUE}}

  # Whoa wtf is this? Well, gRPC encodes to JSON, but numeric values in JSON are capped at 32-bits. So how does Google solve this? Strings!
  # Any precise numeric value that can use more than 32-bits must be encoded for on-the-wire transmission as a string.
  # This also means that non-precise numeric values, such as floats, *do not* need to be encoded as strings.
  def encode(value) when is_integer(value), do: %Google.Protobuf.Value{kind: {:string_value, Integer.to_string(value)}}
  def encode(%Decimal{} = value), do: %Google.Protobuf.Value{kind: {:string_value, Decimal.to_string(value)}}

  # Standard types.
  def encode(value) when is_binary(value), do: %Google.Protobuf.Value{kind: {:string_value, value}}
  def encode(value) when is_float(value), do: %Google.Protobuf.Value{kind: {:number_value, value}}
  def encode(value) when is_boolean(value), do: %Google.Protobuf.Value{kind: {:bool_value, value}}

  # Aggregate types.
  def encode(value) when is_list(value), do: %Google.Protobuf.ListValue{values: Enum.map(value, &encode/1)}
  def encode(value) when is_map(value) do
    fields =
      value
      |> Enum.map(fn
        {key, value} when is_atom(key) ->
          {Atom.to_string(key), encode(value)}

        {key, value} when is_binary(key) ->
          {key, encode(value)}
      end)
      |> Enum.into(%{})
    %Google.Protobuf.Struct{fields: fields}
  end


  @doc """
  Decodes a protobuf "type wrapper" into an Elixir-native term.

  The first argument is expected to be a valid Spanner data type code.

  If the data type code is not recognized or the value is not in a format we know how to convert, the value is returned as-is.
  """
  def decode(_, :NULL_VALUE), do: nil
  def decode(:NUMERIC, value) when is_binary(value), do: Decimal.new(value)
  def decode(:INT64, value) when is_binary(value), do: String.to_integer(value)
  def decode(:FLOAT64, value) when is_binary(value), do: String.to_float(value)
  def decode(:BOOL, "true"), do: true
  def decode(:BOOL, "false"), do: false
  def decode(:STRING, value) when is_binary(value), do: value
  def decode(:BYTES, value) when is_binary(value), do: Base.decode64(value)
  def decode(:TIMESTAMP, value) when is_binary(value), do: DateTime.from_iso8601(value) |> elem(1)
  def decode(:TIMESTAMP, value) when is_number(value), do: DateTime.from_unix!(trunc(value))
  def decode(:DATE, value) when is_binary(value), do: Date.from_iso8601(value) |> elem(1)
  def decode(_, value), do: value
end
