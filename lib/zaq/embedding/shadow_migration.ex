defmodule Zaq.Embedding.ShadowMigration do
  @moduledoc """
  Non-destructive embedding-dimension migration for the `chunks` table.

  A candidate table is prepared and populated while the live table remains in
  service. Cutover and rollback use fixed SQL identifiers and transactional
  `ACCESS EXCLUSIVE` locks so validation and table renames are atomic.
  """

  require Logger

  alias Ecto.Adapters.SQL
  alias Zaq.Embedding.{Client, MigrationLock}
  alias Zaq.Ingestion.DocumentChunker.Chunk, as: DocumentChunk
  alias Zaq.{Repo, System}
  alias Zaq.System.AIProviderCredential

  @live_table "chunks"
  @shadow_table "chunks_embedding_shadow"
  @rollback_table "chunks_embedding_rollback"
  @failed_table "chunks_embedding_failed"
  @max_dimension 4000
  @transaction_timeout 120_000

  @snapshot_keys %{
    old_credential_id: "embedding.shadow_migration.old_credential_id",
    old_provider: "embedding.shadow_migration.old_provider",
    old_endpoint: "embedding.shadow_migration.old_endpoint",
    old_model: "embedding.shadow_migration.old_model",
    old_dimension: "embedding.shadow_migration.old_dimension",
    target_credential_id: "embedding.shadow_migration.target_credential_id",
    target_provider: "embedding.shadow_migration.target_provider",
    target_endpoint: "embedding.shadow_migration.target_endpoint",
    target_model: "embedding.shadow_migration.target_model",
    target_dimension: "embedding.shadow_migration.target_dimension"
  }

  @manifest_keys %{
    credential_id: "embedding.shadow_migration.manifest.credential_id",
    provider: "embedding.shadow_migration.manifest.provider",
    endpoint: "embedding.shadow_migration.manifest.endpoint",
    model: "embedding.shadow_migration.manifest.model",
    dimension: "embedding.shadow_migration.manifest.dimension"
  }

  @max_backfill_retries 3
  @max_retry_delay_seconds 60
  @default_batch_size 64
  @max_batch_size 256

  @shadow_column_signature [
    {"id", "bigint", true, ""},
    {"document_id", "bigint", true, ""},
    {"content", "text", true, ""},
    {"chunk_index", "integer", true, ""},
    {"section_path", "text[]", false, ""},
    {"metadata", "jsonb", false, ""},
    {"language", "character varying(32)", false, ""},
    {"inserted_at", "timestamp(0) without time zone", true, ""},
    {"updated_at", "timestamp(0) without time zone", true, ""},
    {"embedding", :embedding, false, ""},
    {"content_tsv", "tsvector", false, "s"}
  ]

  @shadow_constraint_signature MapSet.new([
                                 {"chunks_embedding_shadow_document_id_fkey", "f", true,
                                  "FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE"},
                                 {"chunks_embedding_shadow_pkey", "p", true, "PRIMARY KEY (id)"}
                               ])

  @scalar_columns "id, document_id, content, chunk_index, section_path, metadata, language, inserted_at, updated_at"

  @doc "Returns table presence, dimensions, and current non-secret embedding selection."
  @spec status() :: map()
  def status do
    %{
      live: relation_status(@live_table),
      shadow: relation_status(@shadow_table),
      rollback: relation_status(@rollback_table),
      failed: relation_status(@failed_table),
      embedding: current_embedding_selection()
    }
  end

  @doc "Creates the empty candidate chunks table for the target dimension."
  @spec prepare(pos_integer()) :: :ok | {:error, term()}
  def prepare(target_dimension) do
    with :ok <- validate_dimension(target_dimension) do
      transaction_result(fn ->
        :ok = MigrationLock.acquire!()
        require_or_rollback(ensure_absent(@rollback_table, :rollback_table_exists))
        require_or_rollback(ensure_absent(@failed_table, :failed_table_exists))
        require_or_rollback(ensure_absent("chunks_bm25_idx", :paradedb_index_present))

        prepare_shadow_table(target_dimension)
      end)
    end
  end

  @doc "Re-embeds live chunks into the shadow table without changing active settings."
  @spec backfill(keyword()) :: {:ok, map()} | {:error, term()}
  def backfill(opts) when is_list(opts) do
    with {:ok, requested_target} <- parse_backfill_target(opts),
         :ok <- ensure_present(@live_table, :live_table_missing),
         :ok <- ensure_present(@shadow_table, :shadow_table_missing),
         true <-
           shadow_schema_valid?(requested_target.dimension) ||
             {:error, :shadow_schema_mismatch},
         {:ok, target} <- establish_backfill_target(requested_target),
         :ok <- delete_shadow_only_rows() do
      run_backfill(target)
    end
  end

  @doc "Discards the candidate table and manifest before cutover."
  @spec abort() :: :ok | {:error, term()}
  def abort do
    transaction_result(fn ->
      :ok = MigrationLock.acquire!()
      require_or_rollback(ensure_present(@live_table, :live_table_missing))
      require_or_rollback(ensure_absent(@rollback_table, :rollback_table_exists))
      require_or_rollback(ensure_absent(@failed_table, :failed_table_exists))
      require_or_rollback(ensure_snapshot_absent())
      require_or_rollback(ensure_present(@shadow_table, :shadow_table_missing))

      SQL.query!(Repo, "DROP TABLE chunks_embedding_shadow", [])
      clear_backfill_manifest!()
      :ok
    end)
  end

  @doc "Validates a completed shadow backfill against the live chunks table."
  @spec validate(pos_integer()) :: :ok | {:error, term()}
  def validate(target_dimension) do
    with :ok <- validate_dimension(target_dimension),
         :ok <- ensure_present(@live_table, :live_table_missing),
         :ok <- ensure_present(@shadow_table, :shadow_table_missing),
         true <- shadow_schema_valid?(target_dimension) || {:error, :shadow_schema_mismatch},
         :ok <- validate_non_null_rows(@live_table),
         :ok <- validate_non_null_rows(@shadow_table),
         :ok <- validate_shadow_dimensions(target_dimension),
         :ok <- validate_row_counts(@live_table, @shadow_table),
         :ok <- validate_id_sets(@live_table, @shadow_table) do
      validate_scalar_rows(@live_table, @shadow_table)
    end
  end

  @doc "Atomically promotes the validated shadow table and records rollback state."
  @spec cutover(keyword()) :: :ok | {:error, term()}
  def cutover(opts) when is_list(opts) do
    with {:ok, requested_target} <- parse_target(opts) do
      transaction_result(fn ->
        :ok = MigrationLock.acquire!()
        target = resolve_cutover_target_or_rollback(requested_target)
        require_or_rollback(ensure_present(@live_table, :live_table_missing))
        require_or_rollback(ensure_present(@shadow_table, :shadow_table_missing))
        require_or_rollback(ensure_absent(@rollback_table, :rollback_table_exists))
        require_or_rollback(ensure_absent(@failed_table, :failed_table_exists))
        require_or_rollback(ensure_snapshot_absent())
        require_or_rollback(validate_cutover_manifest(target))

        SQL.query!(
          Repo,
          "LOCK TABLE chunks, chunks_embedding_shadow IN ACCESS EXCLUSIVE MODE",
          []
        )

        require_or_rollback(ensure_absent("chunks_bm25_idx", :paradedb_index_present))
        require_or_rollback(validate(target.dimension))

        old = current_embedding_selection()
        require_or_rollback(validate_selection(old, :current_embedding_config_incomplete))
        old = selection_with_credential_identity_or_rollback(old)

        write_snapshot!(old, target)
        rename_live_to_rollback!()
        rename_shadow_to_live!()
        advance_live_sequence!()
        put_embedding_selection!(target)
        :ok
      end)
    end
  end

  @doc "Restores the retained pre-cutover table when no post-cutover data changed."
  @spec rollback() :: :ok | {:error, term()}
  def rollback do
    transaction_result(fn ->
      :ok = MigrationLock.acquire!()
      require_or_rollback(ensure_present(@live_table, :live_table_missing))
      require_or_rollback(ensure_present(@rollback_table, :rollback_table_missing))
      require_or_rollback(ensure_absent(@failed_table, :failed_table_exists))

      SQL.query!(
        Repo,
        "LOCK TABLE chunks, chunks_embedding_rollback IN ACCESS EXCLUSIVE MODE",
        []
      )

      snapshot = read_snapshot()
      require_or_rollback(validate_snapshot(snapshot))
      require_or_rollback(validate_snapshot_credential_identities(snapshot))
      require_or_rollback(validate_current_target(snapshot))
      require_or_rollback(validate_scalar_rows_for_rollback())

      rename_live_to_failed!()
      rename_rollback_to_live!()
      advance_live_sequence!()
      restore_old_selection!(snapshot)
      :ok
    end)
  end

  defp transaction_result(fun) do
    case Repo.transaction(fun, timeout: @transaction_timeout) do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  rescue
    Postgrex.Error -> {:error, :embedding_migration_database_error}
  end

  defp require_or_rollback(:ok), do: :ok
  defp require_or_rollback({:error, reason}), do: Repo.rollback(reason)

  defp parse_backfill_target(opts) do
    provider = Keyword.get(opts, :provider)
    endpoint = Keyword.get(opts, :endpoint)
    model = Keyword.get(opts, :model)
    dimension = Keyword.get(opts, :dimension)
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    with :ok <- validate_target_string(provider, :invalid_target_provider),
         :ok <- validate_target_string(endpoint, :invalid_target_endpoint),
         :ok <- validate_target_string(model, :invalid_target_model),
         :ok <- validate_batch_size(batch_size),
         :ok <- validate_dimension(dimension) do
      {:ok,
       %{
         provider: String.trim(provider),
         endpoint: normalize_endpoint(endpoint),
         model: String.trim(model),
         dimension: dimension,
         batch_size: batch_size
       }}
    end
  end

  defp validate_target_string(value, reason) do
    if non_empty_string?(value), do: :ok, else: {:error, reason}
  end

  defp validate_batch_size(batch_size)
       when is_integer(batch_size) and batch_size > 0 and batch_size <= @max_batch_size,
       do: :ok

  defp validate_batch_size(_batch_size), do: {:error, :invalid_batch_size}

  defp resolve_backfill_credential(target) do
    matches =
      AIProviderCredential
      |> Repo.all()
      |> Enum.filter(fn credential ->
        credential.provider == target.provider and
          normalize_endpoint(credential.endpoint) == target.endpoint
      end)

    case matches do
      [credential] -> {:ok, credential}
      [] -> {:error, :embedding_credential_not_found}
      _credentials -> {:error, :ambiguous_embedding_credential}
    end
  end

  defp build_backfill_target(requested_target, credential) do
    requested_target
    |> Map.put(:credential_id, credential.id)
    |> Map.put(:api_key, System.resolve_ai_provider_api_key(credential))
  end

  defp normalize_endpoint(endpoint) when is_binary(endpoint) do
    endpoint
    |> String.trim()
    |> String.trim_trailing("/")
  end

  defp establish_backfill_target(requested_target) do
    case Repo.transaction(
           fn ->
             :ok = MigrationLock.acquire!()

             credential =
               case resolve_backfill_credential(requested_target) do
                 {:ok, credential} -> credential
                 {:error, reason} -> Repo.rollback(reason)
               end

             target = build_backfill_target(requested_target, credential)
             require_or_rollback(write_or_validate_backfill_manifest(target))
             target
           end,
           timeout: @transaction_timeout
         ) do
      {:ok, target} -> {:ok, target}
      {:error, reason} -> {:error, reason}
    end
  rescue
    Postgrex.Error -> {:error, :embedding_migration_database_error}
  end

  defp write_or_validate_backfill_manifest(target) do
    expected = manifest_values(target)
    stored = read_manifest()

    cond do
      Enum.all?(Map.values(stored), &is_nil/1) -> write_manifest_values(expected)
      stored == expected -> :ok
      true -> {:error, :shadow_backfill_target_mismatch}
    end
  end

  defp manifest_values(target) do
    %{
      credential_id: to_string(target.credential_id),
      provider: target.provider,
      endpoint: target.endpoint,
      model: target.model,
      dimension: to_string(target.dimension)
    }
  end

  defp read_manifest do
    Map.new(@manifest_keys, fn {name, key} -> {name, System.get_config(key)} end)
  end

  defp write_manifest_values(values) do
    Enum.each(@manifest_keys, &write_manifest_value(&1, values))
    :ok
  end

  defp write_manifest_value({name, key}, values) do
    case System.set_config(key, Map.fetch!(values, name)) do
      {:ok, _row} -> :ok
      {:error, _reason} -> Repo.rollback(:shadow_manifest_write_failed)
    end
  end

  defp clear_backfill_manifest! do
    SQL.query!(
      Repo,
      "DELETE FROM system_configs WHERE key = ANY($1::text[])",
      [Map.values(@manifest_keys)]
    )
  end

  defp delete_shadow_only_rows do
    case SQL.query(
           Repo,
           """
           DELETE FROM chunks_embedding_shadow AS shadow
           WHERE NOT EXISTS (SELECT 1 FROM chunks AS live WHERE live.id = shadow.id)
           """,
           []
         ) do
      {:ok, _result} -> :ok
      {:error, _reason} -> {:error, :shadow_reconciliation_failed}
    end
  end

  defp run_backfill(target) do
    state = %{
      batches: 0,
      processed: 0,
      retries: 0,
      total: row_count(@live_table)
    }

    continue_backfill(target, state)
  end

  defp continue_backfill(target, state) do
    case pending_backfill_rows(target.batch_size) do
      [] ->
        result = Map.put(state, :remaining, 0)

        Logger.info(
          "Shadow embedding backfill complete processed=#{result.processed} total=#{result.total} batches=#{result.batches} retries=#{result.retries}"
        )

        {:ok, result}

      rows ->
        inputs =
          Enum.map(rows, fn row ->
            DocumentChunk.embedding_input(row.content, row.section_path || [])
          end)

        with {:ok, embeddings, retry_count} <- embed_batch_with_retry(inputs, target, 0),
             :ok <- validate_embedding_dimensions(embeddings, target.dimension),
             :ok <- persist_backfill_batch(rows, embeddings) do
          processed = state.processed + length(rows)
          remaining = max(state.total - processed, 0)

          Logger.info(
            "Shadow embedding backfill progress processed=#{processed} total=#{state.total} remaining=#{remaining}"
          )

          continue_backfill(target, %{
            state
            | batches: state.batches + 1,
              processed: processed,
              retries: state.retries + retry_count
          })
        end
    end
  end

  defp pending_backfill_rows(limit) do
    %Postgrex.Result{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT live.id, live.document_id, live.content, live.chunk_index, live.section_path,
               live.metadata, live.language, live.inserted_at, live.updated_at
        FROM chunks AS live
        LEFT JOIN chunks_embedding_shadow AS shadow ON shadow.id = live.id
        WHERE shadow.id IS NULL
           OR shadow.embedding IS NULL
           OR shadow.document_id IS DISTINCT FROM live.document_id
           OR shadow.content IS DISTINCT FROM live.content
           OR shadow.chunk_index IS DISTINCT FROM live.chunk_index
           OR shadow.section_path IS DISTINCT FROM live.section_path
           OR shadow.metadata IS DISTINCT FROM live.metadata
           OR shadow.language IS DISTINCT FROM live.language
           OR shadow.inserted_at IS DISTINCT FROM live.inserted_at
           OR shadow.updated_at IS DISTINCT FROM live.updated_at
        ORDER BY live.id
        LIMIT $1
        """,
        [limit]
      )

    Enum.map(rows, fn [
                        id,
                        document_id,
                        content,
                        chunk_index,
                        section_path,
                        metadata,
                        language,
                        inserted_at,
                        updated_at
                      ] ->
      %{
        id: id,
        document_id: document_id,
        content: content,
        chunk_index: chunk_index,
        section_path: section_path,
        metadata: metadata,
        language: language,
        inserted_at: inserted_at,
        updated_at: updated_at
      }
    end)
  end

  defp embed_batch_with_retry(inputs, target, retry_count) do
    config = %{endpoint: target.endpoint, api_key: target.api_key, model: target.model}

    case Client.embed_many(inputs, config: config) do
      {:ok, embeddings} ->
        {:ok, embeddings, retry_count}

      {:error, reason} when retry_count < @max_backfill_retries ->
        if retryable_embedding_error?(reason) do
          Process.sleep(retry_delay_ms(reason))
          embed_batch_with_retry(inputs, target, retry_count + 1)
        else
          {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp retryable_embedding_error?({:rate_limited, _delay_seconds, %{status: 429}}), do: true

  defp retryable_embedding_error?({:embedding_http_error, status})
       when status in [408, 425, 500, 502, 503, 504], do: true

  defp retryable_embedding_error?(:embedding_transport_error), do: true
  defp retryable_embedding_error?(_reason), do: false

  defp retry_delay_ms({:rate_limited, delay_seconds, %{status: 429}}) do
    min(delay_seconds, @max_retry_delay_seconds) * 1_000
  end

  defp retry_delay_ms(_reason), do: 1_000

  defp validate_embedding_dimensions(embeddings, expected_dimension) do
    case Enum.find(embeddings, &(length(&1) != expected_dimension)) do
      nil ->
        :ok

      embedding ->
        {:error, {:embedding_dimension_mismatch, expected_dimension, length(embedding)}}
    end
  end

  defp persist_backfill_batch(rows, embeddings) do
    transaction_result(fn ->
      rows
      |> Enum.zip(embeddings)
      |> Enum.each(&persist_backfill_row/1)

      :ok
    end)
  end

  defp persist_backfill_row({row, embedding}) do
    vector = Pgvector.HalfVector.new(embedding)

    case SQL.query(
           Repo,
           """
           INSERT INTO chunks_embedding_shadow
             (id, document_id, content, chunk_index, section_path, metadata, language,
              inserted_at, updated_at, embedding)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::halfvec)
           ON CONFLICT (id) DO UPDATE SET
             document_id = EXCLUDED.document_id,
             content = EXCLUDED.content,
             chunk_index = EXCLUDED.chunk_index,
             section_path = EXCLUDED.section_path,
             metadata = EXCLUDED.metadata,
             language = EXCLUDED.language,
             inserted_at = EXCLUDED.inserted_at,
             updated_at = EXCLUDED.updated_at,
             embedding = EXCLUDED.embedding
           """,
           [
             row.id,
             row.document_id,
             row.content,
             row.chunk_index,
             row.section_path,
             row.metadata,
             row.language,
             row.inserted_at,
             row.updated_at,
             vector
           ]
         ) do
      {:ok, _result} -> :ok
      {:error, _reason} -> Repo.rollback(:shadow_batch_write_failed)
    end
  end

  defp parse_target(opts) do
    credential_id = Keyword.get(opts, :credential_id)
    model = Keyword.get(opts, :model)
    dimension = Keyword.get(opts, :dimension)

    cond do
      not (is_integer(credential_id) and credential_id > 0) ->
        {:error, :invalid_target_credential_id}

      not (is_binary(model) and String.trim(model) != "") ->
        {:error, :invalid_target_model}

      true ->
        case validate_dimension(dimension) do
          :ok ->
            {:ok,
             %{
               credential_id: credential_id,
               model: model,
               dimension: dimension
             }}

          {:error, _reason} = error ->
            error
        end
    end
  end

  defp resolve_cutover_target_or_rollback(requested_target) do
    case System.get_ai_provider_credential(requested_target.credential_id) do
      %AIProviderCredential{} = credential ->
        %{
          credential_id: credential.id,
          provider: credential.provider,
          endpoint: normalize_endpoint(credential.endpoint),
          model: requested_target.model,
          dimension: requested_target.dimension
        }

      nil ->
        Repo.rollback(:target_embedding_credential_not_found)
    end
  end

  defp validate_cutover_manifest(target) do
    stored = read_manifest()

    cond do
      Enum.all?(Map.values(stored), &is_nil/1) -> {:error, :shadow_backfill_manifest_missing}
      stored == manifest_values(target) -> :ok
      true -> {:error, :shadow_backfill_target_mismatch}
    end
  end

  defp validate_dimension(dimension)
       when is_integer(dimension) and dimension > 0 and dimension <= @max_dimension,
       do: :ok

  defp validate_dimension(_dimension), do: {:error, :invalid_embedding_dimension}

  defp prepare_shadow_table(target_dimension) do
    if relation_exists?(@shadow_table) do
      require_or_rollback(validate_shadow_schema(target_dimension))
    else
      create_shadow_table(target_dimension)
      require_or_rollback(validate_shadow_schema(target_dimension))
    end
  end

  defp create_shadow_table(target_dimension) do
    SQL.query!(Repo, "CREATE EXTENSION IF NOT EXISTS vector", [])

    SQL.query!(
      Repo,
      """
      CREATE TABLE chunks_embedding_shadow (
        id bigserial CONSTRAINT chunks_embedding_shadow_pkey PRIMARY KEY,
        document_id bigint NOT NULL,
        content text NOT NULL,
        chunk_index integer NOT NULL,
        section_path text[] DEFAULT '{}',
        metadata jsonb DEFAULT '{}',
        language varchar(32),
        inserted_at timestamp(0) NOT NULL,
        updated_at timestamp(0) NOT NULL,
        embedding halfvec(#{target_dimension}),
        content_tsv tsvector GENERATED ALWAYS AS
          (to_tsvector('english'::regconfig, content)) STORED,
        CONSTRAINT chunks_embedding_shadow_document_id_fkey
          FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
      )
      """,
      []
    )

    SQL.query!(
      Repo,
      """
      CREATE INDEX chunks_embedding_shadow_document_id_index
      ON chunks_embedding_shadow (document_id)
      """,
      []
    )

    SQL.query!(
      Repo,
      """
      CREATE INDEX chunks_embedding_shadow_content_tsv_idx
      ON chunks_embedding_shadow USING gin (content_tsv)
      """,
      []
    )

    SQL.query!(
      Repo,
      """
      CREATE INDEX chunks_embedding_shadow_embedding_idx
      ON chunks_embedding_shadow
      USING hnsw (embedding halfvec_l2_ops)
      WITH (m = 16, ef_construction = 64)
      """,
      []
    )

    :ok
  end

  defp validate_shadow_schema(target_dimension) do
    if shadow_schema_valid?(target_dimension),
      do: :ok,
      else: {:error, {:shadow_schema_mismatch, target_dimension}}
  end

  defp shadow_schema_valid?(target_dimension) do
    table_dimension(@shadow_table) == target_dimension and
      column_signature(@shadow_table) == expected_column_signature(target_dimension) and
      generated_expression(@shadow_table) == "to_tsvector('english'::regconfig, content)" and
      required_relations_exist?([
        "chunks_embedding_shadow_id_seq",
        "chunks_embedding_shadow_pkey",
        "chunks_embedding_shadow_document_id_index",
        "chunks_embedding_shadow_content_tsv_idx",
        "chunks_embedding_shadow_embedding_idx"
      ]) and
      constraint_signature(@shadow_table) == @shadow_constraint_signature and
      index_signature(@shadow_table) == expected_index_signature() and
      shadow_sequence_owned?() and
      shadow_defaults_valid?()
  end

  defp expected_column_signature(target_dimension) do
    Enum.map(@shadow_column_signature, fn
      {name, :embedding, not_null?, generated} ->
        {name, "halfvec(#{target_dimension})", not_null?, generated}

      signature ->
        signature
    end)
  end

  defp shadow_defaults_valid? do
    defaults = column_defaults(@shadow_table)

    defaults == %{
      "chunk_index" => nil,
      "content" => nil,
      "content_tsv" => nil,
      "document_id" => nil,
      "embedding" => nil,
      "id" => "nextval('chunks_embedding_shadow_id_seq'::regclass)",
      "inserted_at" => nil,
      "language" => nil,
      "metadata" => "'{}'::jsonb",
      "section_path" => "'{}'::text[]",
      "updated_at" => nil
    }
  end

  defp validate_non_null_rows(table) when table in [@live_table, @shadow_table] do
    %Postgrex.Result{rows: [[invalid_count]]} =
      SQL.query!(
        Repo,
        "SELECT COUNT(*) FROM #{table} WHERE content IS NULL OR embedding IS NULL",
        []
      )

    if invalid_count == 0, do: :ok, else: {:error, {:null_chunk_values, table, invalid_count}}
  end

  defp validate_shadow_dimensions(target_dimension) do
    %Postgrex.Result{rows: [[invalid_count]]} =
      SQL.query!(
        Repo,
        """
        SELECT COUNT(*)
        FROM chunks_embedding_shadow
        WHERE embedding IS NOT NULL AND vector_dims(embedding) <> $1
        """,
        [target_dimension]
      )

    if invalid_count == 0,
      do: :ok,
      else: {:error, {:embedding_dimension_mismatch, invalid_count}}
  end

  defp validate_row_counts(left, right)
       when left in [@live_table, @shadow_table] and right in [@shadow_table] do
    if row_count(left) == row_count(right), do: :ok, else: {:error, :row_count_mismatch}
  end

  defp validate_id_sets(left, right)
       when left in [@live_table, @shadow_table] and right in [@shadow_table] do
    sql = bidirectional_difference_sql(left, right, "id")
    if difference_exists?(sql), do: {:error, :id_set_mismatch}, else: :ok
  end

  defp validate_scalar_rows(left, right)
       when left in [@live_table, @shadow_table] and right in [@shadow_table] do
    sql = bidirectional_difference_sql(left, right, @scalar_columns)
    if difference_exists?(sql), do: {:error, :scalar_rows_mismatch}, else: :ok
  end

  defp validate_scalar_rows_for_rollback do
    sql = bidirectional_difference_sql(@live_table, @rollback_table, @scalar_columns)
    if difference_exists?(sql), do: {:error, :post_cutover_data_changed}, else: :ok
  end

  defp bidirectional_difference_sql(left, right, columns) do
    """
    SELECT EXISTS (
      (SELECT #{columns} FROM #{left} EXCEPT SELECT #{columns} FROM #{right})
      UNION ALL
      (SELECT #{columns} FROM #{right} EXCEPT SELECT #{columns} FROM #{left})
    )
    """
  end

  defp difference_exists?(sql) do
    %Postgrex.Result{rows: [[exists?]]} = SQL.query!(Repo, sql, [])
    exists?
  end

  defp row_count(table) when table in [@live_table, @shadow_table] do
    %Postgrex.Result{rows: [[count]]} = SQL.query!(Repo, "SELECT COUNT(*) FROM #{table}", [])
    count
  end

  defp rename_live_to_rollback! do
    SQL.query!(Repo, "ALTER TABLE chunks RENAME TO chunks_embedding_rollback", [])

    SQL.query!(
      Repo,
      "ALTER SEQUENCE chunks_id_seq RENAME TO chunks_embedding_rollback_id_seq",
      []
    )

    SQL.query!(
      Repo,
      "ALTER TABLE chunks_embedding_rollback RENAME CONSTRAINT chunks_pkey TO chunks_embedding_rollback_pkey",
      []
    )

    SQL.query!(
      Repo,
      "ALTER TABLE chunks_embedding_rollback RENAME CONSTRAINT chunks_document_id_fkey TO chunks_embedding_rollback_document_id_fkey",
      []
    )

    SQL.query!(
      Repo,
      "ALTER INDEX chunks_document_id_index RENAME TO chunks_embedding_rollback_document_id_index",
      []
    )

    SQL.query!(
      Repo,
      "ALTER INDEX chunks_content_tsv_idx RENAME TO chunks_embedding_rollback_content_tsv_idx",
      []
    )

    SQL.query!(
      Repo,
      "ALTER INDEX chunks_embedding_idx RENAME TO chunks_embedding_rollback_embedding_idx",
      []
    )
  end

  defp rename_shadow_to_live! do
    SQL.query!(Repo, "ALTER TABLE chunks_embedding_shadow RENAME TO chunks", [])
    SQL.query!(Repo, "ALTER SEQUENCE chunks_embedding_shadow_id_seq RENAME TO chunks_id_seq", [])

    SQL.query!(
      Repo,
      "ALTER TABLE chunks RENAME CONSTRAINT chunks_embedding_shadow_pkey TO chunks_pkey",
      []
    )

    SQL.query!(
      Repo,
      "ALTER TABLE chunks RENAME CONSTRAINT chunks_embedding_shadow_document_id_fkey TO chunks_document_id_fkey",
      []
    )

    SQL.query!(
      Repo,
      "ALTER INDEX chunks_embedding_shadow_document_id_index RENAME TO chunks_document_id_index",
      []
    )

    SQL.query!(
      Repo,
      "ALTER INDEX chunks_embedding_shadow_content_tsv_idx RENAME TO chunks_content_tsv_idx",
      []
    )

    SQL.query!(
      Repo,
      "ALTER INDEX chunks_embedding_shadow_embedding_idx RENAME TO chunks_embedding_idx",
      []
    )
  end

  defp rename_live_to_failed! do
    SQL.query!(Repo, "ALTER TABLE chunks RENAME TO chunks_embedding_failed", [])
    SQL.query!(Repo, "ALTER SEQUENCE chunks_id_seq RENAME TO chunks_embedding_failed_id_seq", [])

    SQL.query!(
      Repo,
      "ALTER TABLE chunks_embedding_failed RENAME CONSTRAINT chunks_pkey TO chunks_embedding_failed_pkey",
      []
    )

    SQL.query!(
      Repo,
      "ALTER TABLE chunks_embedding_failed RENAME CONSTRAINT chunks_document_id_fkey TO chunks_embedding_failed_document_id_fkey",
      []
    )

    SQL.query!(
      Repo,
      "ALTER INDEX chunks_document_id_index RENAME TO chunks_embedding_failed_document_id_index",
      []
    )

    SQL.query!(
      Repo,
      "ALTER INDEX chunks_content_tsv_idx RENAME TO chunks_embedding_failed_content_tsv_idx",
      []
    )

    SQL.query!(
      Repo,
      "ALTER INDEX chunks_embedding_idx RENAME TO chunks_embedding_failed_embedding_idx",
      []
    )
  end

  defp rename_rollback_to_live! do
    SQL.query!(Repo, "ALTER TABLE chunks_embedding_rollback RENAME TO chunks", [])

    SQL.query!(
      Repo,
      "ALTER SEQUENCE chunks_embedding_rollback_id_seq RENAME TO chunks_id_seq",
      []
    )

    SQL.query!(
      Repo,
      "ALTER TABLE chunks RENAME CONSTRAINT chunks_embedding_rollback_pkey TO chunks_pkey",
      []
    )

    SQL.query!(
      Repo,
      "ALTER TABLE chunks RENAME CONSTRAINT chunks_embedding_rollback_document_id_fkey TO chunks_document_id_fkey",
      []
    )

    SQL.query!(
      Repo,
      "ALTER INDEX chunks_embedding_rollback_document_id_index RENAME TO chunks_document_id_index",
      []
    )

    SQL.query!(
      Repo,
      "ALTER INDEX chunks_embedding_rollback_content_tsv_idx RENAME TO chunks_content_tsv_idx",
      []
    )

    SQL.query!(
      Repo,
      "ALTER INDEX chunks_embedding_rollback_embedding_idx RENAME TO chunks_embedding_idx",
      []
    )
  end

  defp advance_live_sequence! do
    SQL.query!(
      Repo,
      """
      SELECT setval(
        pg_get_serial_sequence('chunks', 'id')::regclass,
        COALESCE(MAX(id), 1),
        MAX(id) IS NOT NULL
      )
      FROM chunks
      """,
      []
    )
  end

  defp current_embedding_selection do
    %{
      credential_id: System.get_config("embedding.credential_id"),
      model: System.get_config("embedding.model"),
      dimension: System.get_config("embedding.dimension")
    }
  end

  defp selection_with_credential_identity_or_rollback(selection) do
    with {:ok, credential_id} <- parse_credential_id(selection.credential_id),
         %AIProviderCredential{} = credential <-
           System.get_ai_provider_credential(credential_id) do
      Map.merge(selection, %{
        provider: credential.provider,
        endpoint: normalize_endpoint(credential.endpoint)
      })
    else
      _reason -> Repo.rollback(:current_embedding_credential_not_found)
    end
  end

  defp parse_credential_id(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp parse_credential_id(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {credential_id, ""} when credential_id > 0 -> {:ok, credential_id}
      _invalid -> :error
    end
  end

  defp parse_credential_id(_value), do: :error

  defp validate_selection(selection, reason) do
    if Enum.all?(
         [selection.credential_id, selection.model, selection.dimension],
         &non_empty_string?/1
       ),
       do: :ok,
       else: {:error, reason}
  end

  defp non_empty_string?(value), do: is_binary(value) and String.trim(value) != ""

  defp ensure_snapshot_absent do
    if Enum.any?(@snapshot_keys, fn {_name, key} -> System.get_config(key) != nil end),
      do: {:error, :migration_snapshot_exists},
      else: :ok
  end

  defp write_snapshot!(old, target) do
    values = %{
      old_credential_id: old.credential_id,
      old_provider: old.provider,
      old_endpoint: old.endpoint,
      old_model: old.model,
      old_dimension: old.dimension,
      target_credential_id: target.credential_id,
      target_provider: target.provider,
      target_endpoint: target.endpoint,
      target_model: target.model,
      target_dimension: target.dimension
    }

    for {name, key} <- @snapshot_keys do
      {:ok, _row} = System.set_config(key, Map.fetch!(values, name))
    end

    :ok
  end

  defp read_snapshot do
    Map.new(@snapshot_keys, fn {name, key} -> {name, System.get_config(key)} end)
  end

  defp validate_snapshot(snapshot) do
    if Enum.all?(Map.values(snapshot), &non_empty_string?/1),
      do: :ok,
      else: {:error, :migration_snapshot_missing}
  end

  defp validate_snapshot_credential_identities(snapshot) do
    with :ok <-
           validate_snapshot_credential_identity(
             snapshot.old_credential_id,
             snapshot.old_provider,
             snapshot.old_endpoint
           ),
         :ok <-
           validate_snapshot_credential_identity(
             snapshot.target_credential_id,
             snapshot.target_provider,
             snapshot.target_endpoint
           ) do
      :ok
    else
      _reason -> {:error, :rollback_credential_identity_mismatch}
    end
  end

  defp validate_snapshot_credential_identity(credential_id, provider, endpoint) do
    with {:ok, parsed_id} <- parse_credential_id(credential_id),
         %AIProviderCredential{} = credential <- System.get_ai_provider_credential(parsed_id),
         true <- credential.provider == provider,
         true <- normalize_endpoint(credential.endpoint) == endpoint do
      :ok
    else
      _reason -> {:error, :rollback_credential_identity_mismatch}
    end
  end

  defp validate_current_target(snapshot) do
    current = current_embedding_selection()

    target = %{
      credential_id: snapshot.target_credential_id,
      model: snapshot.target_model,
      dimension: snapshot.target_dimension
    }

    if current == target, do: :ok, else: {:error, :embedding_config_drifted}
  end

  defp put_embedding_selection!(selection) do
    {:ok, _} = System.set_config("embedding.credential_id", selection.credential_id)
    {:ok, _} = System.set_config("embedding.model", selection.model)
    {:ok, _} = System.set_config("embedding.dimension", selection.dimension)
    :ok
  end

  defp restore_old_selection!(snapshot) do
    put_embedding_selection!(%{
      credential_id: snapshot.old_credential_id,
      model: snapshot.old_model,
      dimension: snapshot.old_dimension
    })
  end

  defp relation_status(table) do
    if relation_exists?(table) do
      %{present: true, dimension: table_dimension(table), rows: unrestricted_row_count(table)}
    else
      %{present: false, dimension: nil, rows: nil}
    end
  end

  defp relation_exists?(table) do
    %Postgrex.Result{rows: [[exists?]]} =
      SQL.query!(Repo, "SELECT to_regclass($1::text) IS NOT NULL", [table])

    exists?
  end

  defp ensure_present(table, reason) do
    if relation_exists?(table), do: :ok, else: {:error, reason}
  end

  defp ensure_absent(table, reason) do
    if relation_exists?(table), do: {:error, reason}, else: :ok
  end

  defp table_dimension(table) do
    %Postgrex.Result{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT format_type(attribute.atttypid, attribute.atttypmod)
        FROM pg_attribute AS attribute
        WHERE attribute.attrelid = to_regclass($1::text)
          AND attribute.attname = 'embedding'
          AND NOT attribute.attisdropped
        """,
        [table]
      )

    case rows do
      [[type]] ->
        case Regex.run(~r/^halfvec\((\d+)\)$/, type) do
          [_, dimension] -> String.to_integer(dimension)
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp column_signature(table) do
    %Postgrex.Result{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT attribute.attname,
               format_type(attribute.atttypid, attribute.atttypmod),
               attribute.attnotnull,
               attribute.attgenerated::text
        FROM pg_attribute AS attribute
        WHERE attribute.attrelid = to_regclass($1::text)
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped
        ORDER BY attribute.attnum
        """,
        [table]
      )

    Enum.map(rows, &List.to_tuple/1)
  end

  defp column_defaults(table) do
    %Postgrex.Result{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT column_name, column_default
        FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = $1
        """,
        [table]
      )

    Map.new(rows, fn [name, default] -> {name, default} end)
  end

  defp generated_expression(table) do
    %Postgrex.Result{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT pg_get_expr(defaults.adbin, defaults.adrelid)
        FROM pg_attribute AS attribute
        JOIN pg_attrdef AS defaults
          ON defaults.adrelid = attribute.attrelid
         AND defaults.adnum = attribute.attnum
        WHERE attribute.attrelid = to_regclass($1::text)
          AND attribute.attname = 'content_tsv'
        """,
        [table]
      )

    case rows do
      [[expression]] -> expression
      _ -> nil
    end
  end

  defp constraint_signature(table) do
    %Postgrex.Result{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT constraint_record.conname,
               constraint_record.contype::text,
               constraint_record.convalidated,
               pg_get_constraintdef(constraint_record.oid, true)
        FROM pg_constraint AS constraint_record
        WHERE constraint_record.conrelid = to_regclass($1::text)
          AND constraint_record.contype IN ('p', 'f')
        """,
        [table]
      )

    MapSet.new(rows, &List.to_tuple/1)
  end

  defp expected_index_signature do
    [
      {"chunks_embedding_shadow_content_tsv_idx", "gin", false, false, true, true, 1, 1,
       "content_tsv", "tsvector_ops", []},
      {"chunks_embedding_shadow_document_id_index", "btree", false, false, true, true, 1, 1,
       "document_id", "int8_ops", []},
      {"chunks_embedding_shadow_embedding_idx", "hnsw", false, false, true, true, 1, 1,
       "embedding", "halfvec_l2_ops", ["ef_construction=64", "m=16"]},
      {"chunks_embedding_shadow_pkey", "btree", true, true, true, true, 1, 1, "id", "int8_ops",
       []}
    ]
  end

  defp index_signature(table) do
    %Postgrex.Result{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT index_relation.relname,
               access_method.amname,
               index_record.indisunique,
               index_record.indisprimary,
               index_record.indisvalid,
               index_record.indisready,
               index_record.indnkeyatts,
               index_record.indnatts,
               pg_get_indexdef(index_record.indexrelid, 1, true),
               operator_class.opcname,
               COALESCE(index_relation.reloptions, ARRAY[]::text[])
        FROM pg_index AS index_record
        JOIN pg_class AS index_relation ON index_relation.oid = index_record.indexrelid
        JOIN pg_am AS access_method ON access_method.oid = index_relation.relam
        JOIN LATERAL unnest(index_record.indclass::oid[]) WITH ORDINALITY
          AS operator_classes(operator_class_oid, position) ON operator_classes.position = 1
        JOIN pg_opclass AS operator_class
          ON operator_class.oid = operator_classes.operator_class_oid
        WHERE index_record.indrelid = to_regclass($1::text)
        ORDER BY index_relation.relname
        """,
        [table]
      )

    Enum.map(rows, fn row ->
      row
      |> List.update_at(10, &Enum.sort/1)
      |> List.to_tuple()
    end)
  end

  defp shadow_sequence_owned? do
    %Postgrex.Result{rows: [[owned?]]} =
      SQL.query!(
        Repo,
        """
        SELECT EXISTS (
          SELECT 1
          FROM pg_class AS sequence_relation
          JOIN pg_depend AS dependency
            ON dependency.objid = sequence_relation.oid
           AND dependency.classid = 'pg_class'::regclass
           AND dependency.refclassid = 'pg_class'::regclass
          JOIN pg_class AS table_relation ON table_relation.oid = dependency.refobjid
          JOIN pg_attribute AS attribute
            ON attribute.attrelid = table_relation.oid
           AND attribute.attnum = dependency.refobjsubid
          WHERE sequence_relation.oid = to_regclass('chunks_embedding_shadow_id_seq')
            AND sequence_relation.relkind = 'S'
            AND table_relation.oid = to_regclass('chunks_embedding_shadow')
            AND attribute.attname = 'id'
            AND dependency.deptype = 'a'
        )
        """,
        []
      )

    owned?
  end

  defp required_relations_exist?(names), do: Enum.all?(names, &relation_exists?/1)

  defp unrestricted_row_count(table) do
    sql =
      case table do
        @live_table -> "SELECT COUNT(*) FROM chunks"
        @shadow_table -> "SELECT COUNT(*) FROM chunks_embedding_shadow"
        @rollback_table -> "SELECT COUNT(*) FROM chunks_embedding_rollback"
        @failed_table -> "SELECT COUNT(*) FROM chunks_embedding_failed"
      end

    %Postgrex.Result{rows: [[count]]} = SQL.query!(Repo, sql, [])
    count
  end
end
