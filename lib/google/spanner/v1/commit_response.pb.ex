defmodule Google.Spanner.V1.CommitResponse.CommitStats do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:mutation_count, 1, type: :int64, json_name: "mutationCount")
end

defmodule Google.Spanner.V1.CommitResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field(:commit_timestamp, 1, type: Google.Protobuf.Timestamp, json_name: "commitTimestamp")

  field(:commit_stats, 2,
    type: Google.Spanner.V1.CommitResponse.CommitStats,
    json_name: "commitStats"
  )
end
