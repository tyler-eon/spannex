defmodule Spannex.MixProject do
  use Mix.Project

  def project do
    [
      app: :spannex,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:db_connection, "~> 2.6"},
      {:goth, "~> 1.4"},
      {:google_protos, "~> 0.4"},
      {:grpc, "~> 0.7"}
    ]
  end
end
