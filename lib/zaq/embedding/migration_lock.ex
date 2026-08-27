defmodule Zaq.Embedding.MigrationLock do
  @moduledoc false

  alias Ecto.Adapters.SQL
  alias Zaq.Repo

  @lock_key 7_190_977_496_160_145_225

  @doc "Serializes embedding configuration changes and shadow-table lifecycle operations."
  @spec acquire!() :: :ok
  def acquire! do
    SQL.query!(Repo, "SELECT pg_advisory_xact_lock($1)", [@lock_key])
    :ok
  end

  @doc "Returns true while any shadow-migration relation remains present."
  @spec migration_relation_present?() :: boolean()
  def migration_relation_present? do
    %Postgrex.Result{rows: [[present?]]} =
      SQL.query!(
        Repo,
        """
        SELECT to_regclass('chunks_embedding_shadow') IS NOT NULL
            OR to_regclass('chunks_embedding_rollback') IS NOT NULL
            OR to_regclass('chunks_embedding_failed') IS NOT NULL
        """,
        []
      )

    present?
  end
end
