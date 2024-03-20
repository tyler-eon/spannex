defmodule Spannex.MixProject do
  use Mix.Project

  def project do
    [
      app: :spannex,
      version: "0.6.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: """
      Elixir client for the Google Cloud Spanner database using gRPC.
      """,
      package: package(),
      docs: docs(),
      source_url: "https://github.com/tyler-eon/spannex",
      homepage_url: "https://github.com/tyler-eon/spannex"
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
      {:grpc, "~> 0.7"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  # For Hex documentation.
  defp docs do
    [
      main: "Spannex",
      extras: [
        "README.md",
        "LICENSE"
      ]
    ]
  end

  # For Hex packaging.
  defp package do
    [
      links: %{
        "GitHub" => "https://github.com/tyler-eon/spannex"
      },
      licenses: ["Apache-2.0"]
    ]
  end
end
