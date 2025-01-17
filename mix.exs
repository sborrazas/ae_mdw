defmodule AeMdw.MixProject do
  use Mix.Project

  def project() do
    [
      app: :ae_mdw,
      version: "1.0.9",
      elixir: "~> 1.5",
      elixirc_paths: elixirc_paths(Mix.env()),
      compilers: [:phoenix, :gettext] ++ Mix.compilers() ++ [:phoenix_swagger],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      xref: [exclude: [:mnesia, :aec_chain, :aec_blocks, :aec_headers, :aec_sync, :aetx,
		       :aeser_api_encoder, :aec_hash]]
    ]
  end

  # Configuration for the OTP application.
  #
  # Type `mix help compile.app` for more information.
  def application() do
    [
      mod: {AeMdw.Application, []},
      extra_applications: [:logger, :runtime_tools]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Specifies your project dependencies.
  #
  # Type `mix help deps` for examples and options.
  defp deps() do
    [
      {:ae_plugin, github: "aeternity/ae_plugin"},
      # {:aesophia, path: "deps/aesophia", app: false},
      {:stream_split, "~> 0.1.5"},
      {:ex2ms, "~> 1.6.0"},
      {:logger_file_backend, "~> 0.0.11"},
      {:smart_global, github: "ks/smart_global"},
      {:smart_record, github: "ks/smart_record"},
      {:dbg, github: "fishcakez/dbg"},
      {:phoenix, "~> 1.5.8"},
      {:plug, "~> 1.11"},
      {:cors_plug, "~> 2.0"},
      {:gettext, "~> 0.11"},
      {:jason, "~> 1.0"},
      {:plug_cowboy, "~> 2.0"},
      {:riverside, "~> 1.2.6"},
      {:websockex, "~> 0.4.2"},
      {:phoenix_swagger, "~> 0.8"},
      {:temp, "~> 0.4"},
      {:tesla, "~> 1.3.0"}
    ]
  end
end
