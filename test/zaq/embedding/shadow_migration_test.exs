defmodule Zaq.Embedding.ShadowMigrationTest do
  use Zaq.DataCase, async: false

  alias Zaq.Embedding.{Client, ShadowMigration}
  alias Zaq.Ingestion.{Chunk, Document}
  alias Zaq.{Repo, System, SystemConfigFixtures}

  @source_dimension 3
  @target_dimension 4
  @migration_lock_key 7_190_977_496_160_145_225

  setup do
    for table <-
          ~w(chunks_embedding_failed chunks_embedding_rollback chunks_embedding_shadow chunks) do
      Repo.query!("DROP TABLE IF EXISTS #{table} CASCADE", [])
    end

    :ok = Chunk.create_table(@source_dimension)

    old_credential = SystemConfigFixtures.ai_credential_fixture(%{name: "Old embedding"})
    target_credential = SystemConfigFixtures.ai_credential_fixture(%{name: "Target embedding"})

    set_embedding_config(old_credential.id, "old-model", @source_dimension)

    {:ok, document} =
      Document.create(%{
        source: "shadow-migration-#{Ecto.UUID.generate()}.md",
        content: "Original document content"
      })

    source_id = insert_chunk("chunks", document.id, "Original chunk", @source_dimension)

    %{
      document: document,
      old_credential: old_credential,
      source_id: source_id,
      target_credential: target_credential
    }
  end

  test "prepare creates the exact shadow schema and is idempotent" do
    assert :ok = ShadowMigration.prepare(@target_dimension)
    assert :ok = ShadowMigration.prepare(@target_dimension)

    assert current_dimension("chunks_embedding_shadow") == @target_dimension

    assert generated_expression("chunks_embedding_shadow") ==
             "to_tsvector('english'::regconfig, content)"

    mismatched_dimension = @target_dimension + 1

    assert {:error, {:shadow_schema_mismatch, ^mismatched_dimension}} =
             ShadowMigration.prepare(mismatched_dimension)
  end

  test "prepare rejects tampered column definitions" do
    cases = [
      ["ALTER TABLE chunks_embedding_shadow ALTER COLUMN language TYPE varchar(64)"],
      ["ALTER TABLE chunks_embedding_shadow ALTER COLUMN content DROP NOT NULL"],
      [
        "ALTER TABLE chunks_embedding_shadow ALTER COLUMN metadata SET DEFAULT '{\"tampered\":true}'::jsonb"
      ],
      [
        "DROP INDEX chunks_embedding_shadow_content_tsv_idx",
        "ALTER TABLE chunks_embedding_shadow DROP COLUMN content_tsv",
        "ALTER TABLE chunks_embedding_shadow ADD COLUMN content_tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, content)) STORED",
        "CREATE INDEX chunks_embedding_shadow_content_tsv_idx ON chunks_embedding_shadow USING gin (content_tsv)"
      ]
    ]

    for statements <- cases do
      reset_shadow!()
      Enum.each(statements, &Repo.query!(&1, []))

      assert {:error, {:shadow_schema_mismatch, @target_dimension}} =
               ShadowMigration.prepare(@target_dimension)
    end
  end

  test "prepare rejects tampered primary and foreign-key constraints" do
    cases = [
      [
        "ALTER TABLE chunks_embedding_shadow DROP CONSTRAINT chunks_embedding_shadow_document_id_fkey",
        "ALTER TABLE chunks_embedding_shadow ADD CONSTRAINT chunks_embedding_shadow_document_id_fkey FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE RESTRICT"
      ],
      [
        "ALTER TABLE chunks_embedding_shadow DROP CONSTRAINT chunks_embedding_shadow_pkey",
        "ALTER TABLE chunks_embedding_shadow ADD CONSTRAINT chunks_embedding_shadow_pkey PRIMARY KEY (document_id, id)"
      ]
    ]

    for statements <- cases do
      reset_shadow!()
      Enum.each(statements, &Repo.query!(&1, []))

      assert {:error, {:shadow_schema_mismatch, @target_dimension}} =
               ShadowMigration.prepare(@target_dimension)
    end
  end

  test "prepare rejects tampered index method, opclass, options, and validity" do
    cases = [
      [
        "DROP INDEX chunks_embedding_shadow_document_id_index",
        "CREATE INDEX chunks_embedding_shadow_document_id_index ON chunks_embedding_shadow USING hash (document_id)"
      ],
      [
        "DROP INDEX chunks_embedding_shadow_embedding_idx",
        "CREATE INDEX chunks_embedding_shadow_embedding_idx ON chunks_embedding_shadow USING hnsw (embedding halfvec_cosine_ops) WITH (m = 16, ef_construction = 64)"
      ],
      ["ALTER INDEX chunks_embedding_shadow_embedding_idx SET (m = 32)"],
      [
        "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'chunks_embedding_shadow_embedding_idx'::regclass"
      ]
    ]

    for statements <- cases do
      reset_shadow!()
      Enum.each(statements, &Repo.query!(&1, []))

      assert {:error, {:shadow_schema_mismatch, @target_dimension}} =
               ShadowMigration.prepare(@target_dimension)
    end
  end

  test "prepare rejects a shadow sequence without id-column ownership" do
    reset_shadow!()
    Repo.query!("ALTER SEQUENCE chunks_embedding_shadow_id_seq OWNED BY NONE", [])

    assert {:error, {:shadow_schema_mismatch, @target_dimension}} =
             ShadowMigration.prepare(@target_dimension)
  end

  test "prepare refuses to omit an existing ParadeDB index" do
    Repo.query!("CREATE INDEX chunks_bm25_idx ON chunks (id)", [])

    assert {:error, :paradedb_index_present} = ShadowMigration.prepare(@target_dimension)
    refute table_exists?("chunks_embedding_shadow")
  end

  test "prepare rolls back a partial shadow table after an index-name collision" do
    Repo.query!("CREATE INDEX chunks_embedding_shadow_document_id_index ON chunks (id)", [])

    assert {:error, :embedding_migration_database_error} =
             ShadowMigration.prepare(@target_dimension)

    refute table_exists?("chunks_embedding_shadow")
    assert table_exists?("chunks")
    assert table_exists?("chunks_embedding_shadow_document_id_index")
  end

  test "validate requires exact scalar rows and target embeddings", %{document: document} do
    assert :ok = ShadowMigration.prepare(@target_dimension)
    copy_source_to_shadow(@target_dimension)
    assert :ok = ShadowMigration.validate(@target_dimension)

    Repo.query!(
      "UPDATE chunks_embedding_shadow SET content = 'Changed' WHERE document_id = $1",
      [document.id]
    )

    assert {:error, :scalar_rows_mismatch} = ShadowMigration.validate(@target_dimension)
  end

  test "cutover refuses a missing backfill manifest", %{
    old_credential: old_credential,
    target_credential: target_credential
  } do
    assert :ok = ShadowMigration.prepare(@target_dimension)
    copy_source_to_shadow(@target_dimension)

    assert {:error, :shadow_backfill_manifest_missing} =
             ShadowMigration.cutover(
               credential_id: target_credential.id,
               model: "target-model",
               dimension: @target_dimension
             )

    assert current_dimension("chunks") == @source_dimension
    assert current_dimension("chunks_embedding_shadow") == @target_dimension
    assert System.get_config("embedding.credential_id") == to_string(old_credential.id)
    refute table_exists?("chunks_embedding_rollback")
  end

  test "cutover swaps tables, updates config, and advances the canonical sequence", %{
    document: document,
    source_id: source_id,
    target_credential: target_credential
  } do
    assert :ok = ShadowMigration.prepare(@target_dimension)
    copy_source_to_shadow(@target_dimension)
    set_shadow_manifest(target_credential)

    assert :ok =
             ShadowMigration.cutover(
               credential_id: target_credential.id,
               model: "target-model",
               dimension: @target_dimension
             )

    assert current_dimension("chunks") == @target_dimension
    assert current_dimension("chunks_embedding_rollback") == @source_dimension
    assert System.get_config("embedding.credential_id") == to_string(target_credential.id)
    assert System.get_config("embedding.model") == "target-model"
    assert System.get_config("embedding.dimension") == to_string(@target_dimension)
    assert Repo.get!(Document, document.id).content == "Original document content"

    next_id = insert_chunk("chunks", document.id, "Post-cutover chunk", @target_dimension)
    assert next_id > source_id
  end

  test "cutover refuses a late ParadeDB index without mutation", %{
    old_credential: old_credential,
    target_credential: target_credential
  } do
    assert :ok = ShadowMigration.prepare(@target_dimension)
    copy_source_to_shadow(@target_dimension)
    set_shadow_manifest(target_credential)
    Repo.query!("CREATE INDEX chunks_bm25_idx ON chunks (id)", [])

    assert {:error, :paradedb_index_present} =
             ShadowMigration.cutover(
               credential_id: target_credential.id,
               model: "target-model",
               dimension: @target_dimension
             )

    assert current_dimension("chunks") == @source_dimension
    assert current_dimension("chunks_embedding_shadow") == @target_dimension
    assert System.get_config("embedding.credential_id") == to_string(old_credential.id)
    assert System.get_config("embedding.model") == "old-model"
    assert System.get_config("embedding.dimension") == to_string(@source_dimension)
    assert table_exists?("chunks_bm25_idx")
    refute table_exists?("chunks_embedding_rollback")
  end

  test "rollback restores the old table and config while retaining the failed table", %{
    old_credential: old_credential,
    target_credential: target_credential
  } do
    assert :ok = ShadowMigration.prepare(@target_dimension)
    copy_source_to_shadow(@target_dimension)
    set_shadow_manifest(target_credential)

    assert :ok =
             ShadowMigration.cutover(
               credential_id: target_credential.id,
               model: "target-model",
               dimension: @target_dimension
             )

    assert :ok = ShadowMigration.rollback()

    assert current_dimension("chunks") == @source_dimension
    assert current_dimension("chunks_embedding_failed") == @target_dimension
    refute table_exists?("chunks_embedding_rollback")
    assert System.get_config("embedding.credential_id") == to_string(old_credential.id)
    assert System.get_config("embedding.model") == "old-model"
    assert System.get_config("embedding.dimension") == to_string(@source_dimension)
  end

  test "rollback refuses credential identity drift", %{
    old_credential: old_credential,
    target_credential: target_credential
  } do
    assert :ok = ShadowMigration.prepare(@target_dimension)
    copy_source_to_shadow(@target_dimension)
    set_shadow_manifest(target_credential)

    assert :ok =
             ShadowMigration.cutover(
               credential_id: target_credential.id,
               model: "target-model",
               dimension: @target_dimension
             )

    Repo.query!(
      "UPDATE ai_provider_credentials SET endpoint = $1 WHERE id = $2",
      ["https://changed-old.example/v1", old_credential.id]
    )

    assert {:error, :rollback_credential_identity_mismatch} = ShadowMigration.rollback()
    assert current_dimension("chunks") == @target_dimension
    assert current_dimension("chunks_embedding_rollback") == @source_dimension
    refute table_exists?("chunks_embedding_failed")
  end

  test "rollback refuses to discard post-cutover data", %{
    document: document,
    target_credential: target_credential
  } do
    assert :ok = ShadowMigration.prepare(@target_dimension)
    copy_source_to_shadow(@target_dimension)
    set_shadow_manifest(target_credential)

    assert :ok =
             ShadowMigration.cutover(
               credential_id: target_credential.id,
               model: "target-model",
               dimension: @target_dimension
             )

    _new_id = insert_chunk("chunks", document.id, "New live chunk", @target_dimension)

    assert {:error, :post_cutover_data_changed} = ShadowMigration.rollback()
    assert table_exists?("chunks")
    assert table_exists?("chunks_embedding_rollback")
    refute table_exists?("chunks_embedding_failed")
  end

  test "backfill uses section-aware batch input and resumes without duplicate requests", %{
    source_id: source_id
  } do
    credential = shadow_credential_fixture()
    assert :ok = ShadowMigration.prepare(@target_dimension)

    Repo.query!(
      "UPDATE chunks SET section_path = ARRAY['Section', 'Topic'] WHERE id = $1",
      [source_id]
    )

    Req.Test.stub(Client, fn conn ->
      assert Plug.Conn.get_req_header(conn, "authorization") == ["Bearer shadow-key"]
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      decoded = Jason.decode!(body)
      assert decoded["model"] == "target-model"
      assert decoded["input"] == ["Section > Topic\n\nOriginal chunk"]

      Req.Test.json(conn, %{
        "data" => [%{"index" => 0, "embedding" => List.duplicate(0.2, @target_dimension)}]
      })
    end)

    opts = backfill_opts(credential)

    assert {:ok, %{batches: 1, processed: 1, remaining: 0, retries: 0, total: 1}} =
             ShadowMigration.backfill(opts)

    assert %Postgrex.Result{rows: [["Original chunk", ["Section", "Topic"], @target_dimension]]} =
             Repo.query!(
               "SELECT content, section_path, vector_dims(embedding) FROM chunks_embedding_shadow WHERE id = $1",
               [source_id]
             )

    Req.Test.stub(Client, fn _conn ->
      flunk("completed shadow rows must not be embedded again")
    end)

    assert {:ok, %{batches: 0, processed: 0, remaining: 0, retries: 0, total: 1}} =
             ShadowMigration.backfill(opts)
  end

  test "backfill reconciles changed live rows and removes shadow extras", %{
    document: document,
    source_id: source_id
  } do
    credential = shadow_credential_fixture()
    assert :ok = ShadowMigration.prepare(@target_dimension)
    copy_source_to_shadow(@target_dimension)

    Repo.query!("UPDATE chunks SET content = 'Changed live chunk' WHERE id = $1", [source_id])

    extra_vector = Pgvector.HalfVector.new(List.duplicate(0.3, @target_dimension))

    Repo.query!(
      """
      INSERT INTO chunks_embedding_shadow
        (id, document_id, content, chunk_index, section_path, metadata, embedding, language,
         inserted_at, updated_at)
      VALUES (999999, $1, 'Stale extra', 99, '{}', '{}', $2::halfvec, 'fr', NOW(), NOW())
      """,
      [document.id, extra_vector]
    )

    Req.Test.stub(Client, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      assert Jason.decode!(body)["input"] == ["Changed live chunk"]

      Req.Test.json(conn, %{
        "data" => [%{"index" => 0, "embedding" => List.duplicate(0.4, @target_dimension)}]
      })
    end)

    assert {:ok, %{processed: 1, remaining: 0, total: 1}} =
             ShadowMigration.backfill(backfill_opts(credential))

    assert %Postgrex.Result{rows: [["Changed live chunk"]]} =
             Repo.query!("SELECT content FROM chunks_embedding_shadow WHERE id = $1", [source_id])

    assert %Postgrex.Result{rows: [[0]]} =
             Repo.query!("SELECT COUNT(*) FROM chunks_embedding_shadow WHERE id = 999999", [])
  end

  test "backfill preserves nullable scalar fields exactly", %{source_id: source_id} do
    credential = shadow_credential_fixture()
    assert :ok = ShadowMigration.prepare(@target_dimension)

    Repo.query!(
      "UPDATE chunks SET section_path = NULL, metadata = NULL WHERE id = $1",
      [source_id]
    )

    Process.put(:nullable_backfill_requests, 0)

    Req.Test.stub(Client, fn conn ->
      request_count = Process.get(:nullable_backfill_requests, 0) + 1
      Process.put(:nullable_backfill_requests, request_count)
      assert request_count == 1

      {:ok, body, conn} = Plug.Conn.read_body(conn)
      assert Jason.decode!(body)["input"] == ["Original chunk"]

      Req.Test.json(conn, %{
        "data" => [%{"index" => 0, "embedding" => List.duplicate(0.2, @target_dimension)}]
      })
    end)

    assert {:ok, %{processed: 1, remaining: 0}} =
             ShadowMigration.backfill(backfill_opts(credential))

    assert %Postgrex.Result{rows: [[nil, nil]]} =
             Repo.query!(
               "SELECT section_path, metadata FROM chunks_embedding_shadow WHERE id = $1",
               [source_id]
             )

    assert :ok = ShadowMigration.validate(@target_dimension)
  end

  test "backfill rejects wrong-dimensional provider vectors without inserting", %{
    source_id: source_id
  } do
    credential = shadow_credential_fixture()
    assert :ok = ShadowMigration.prepare(@target_dimension)

    Req.Test.stub(Client, fn conn ->
      Req.Test.json(conn, %{
        "data" => [%{"index" => 0, "embedding" => List.duplicate(0.2, @target_dimension - 1)}]
      })
    end)

    assert {:error, {:embedding_dimension_mismatch, @target_dimension, 3}} =
             ShadowMigration.backfill(backfill_opts(credential))

    assert %Postgrex.Result{rows: [[0]]} =
             Repo.query!("SELECT COUNT(*) FROM chunks_embedding_shadow WHERE id = $1", [source_id])
  end

  test "backfill retries a zero-delay rate limit once", %{source_id: source_id} do
    credential = shadow_credential_fixture()
    assert :ok = ShadowMigration.prepare(@target_dimension)
    Process.put(:shadow_backfill_attempts, 0)

    Req.Test.stub(Client, fn conn ->
      attempt = Process.get(:shadow_backfill_attempts, 0) + 1
      Process.put(:shadow_backfill_attempts, attempt)

      if attempt == 1 do
        conn
        |> Plug.Conn.put_resp_header("retry-after", "0")
        |> Plug.Conn.put_status(429)
        |> Req.Test.json(%{"error" => "rate limited"})
      else
        Req.Test.json(conn, %{
          "data" => [%{"index" => 0, "embedding" => List.duplicate(0.2, @target_dimension)}]
        })
      end
    end)

    assert {:ok, %{processed: 1, remaining: 0, retries: 1}} =
             ShadowMigration.backfill(backfill_opts(credential))

    assert Process.get(:shadow_backfill_attempts) == 2

    assert %Postgrex.Result{rows: [[1]]} =
             Repo.query!("SELECT COUNT(*) FROM chunks_embedding_shadow WHERE id = $1", [source_id])
  end

  test "backfill and cutover refuse target drift after shadow rows exist" do
    credential = shadow_credential_fixture()
    assert :ok = ShadowMigration.prepare(@target_dimension)

    Req.Test.stub(Client, fn conn ->
      Req.Test.json(conn, %{
        "data" => [%{"index" => 0, "embedding" => List.duplicate(0.2, @target_dimension)}]
      })
    end)

    opts = backfill_opts(credential)
    assert {:ok, %{processed: 1}} = ShadowMigration.backfill(opts)

    Req.Test.stub(Client, fn _conn ->
      flunk("target drift must fail before an HTTP request")
    end)

    assert {:error, :shadow_backfill_target_mismatch} =
             ShadowMigration.backfill(Keyword.put(opts, :model, "different-model"))

    assert {:error, :shadow_backfill_target_mismatch} =
             ShadowMigration.cutover(
               credential_id: credential.id,
               model: "different-model",
               dimension: @target_dimension
             )

    assert table_exists?("chunks")
    assert table_exists?("chunks_embedding_shadow")
    refute table_exists?("chunks_embedding_rollback")
  end

  test "backfill resolves its target credential after waiting for the migration lock" do
    create_shadow_without_migration_lock(@target_dimension)

    {:ok, connection} = Postgrex.start_link(direct_postgrex_options())
    endpoint = "https://locked-embedding-#{Ecto.UUID.generate()}.example/v1"
    name = "Locked embedding #{Ecto.UUID.generate()}"

    %Postgrex.Result{rows: [[credential_id]]} =
      Postgrex.query!(
        connection,
        """
        INSERT INTO ai_provider_credentials
          (name, provider, endpoint, metadata, sovereign, inserted_at, updated_at)
        VALUES ($1, 'scaleway', $2, '{}', false, NOW(), NOW())
        RETURNING id
        """,
        [name, endpoint]
      )

    try do
      Postgrex.query!(connection, "BEGIN", [])
      Postgrex.query!(connection, "SELECT pg_advisory_xact_lock($1)", [@migration_lock_key])

      Req.Test.stub(Client, fn conn ->
        Req.Test.json(conn, %{
          "data" => [%{"index" => 0, "embedding" => List.duplicate(0.2, @target_dimension)}]
        })
      end)

      caller = self()

      backfill_pid =
        spawn(fn ->
          opts =
            backfill_opts(%{endpoint: endpoint})
            |> Keyword.put(:endpoint, endpoint)

          send(caller, {:backfill_result, ShadowMigration.backfill(opts)})
        end)

      wait_for_advisory_waiters(connection, 1)
      Req.Test.allow(Client, self(), backfill_pid)

      assert %Postgrex.Result{num_rows: 1} =
               Postgrex.query!(
                 connection,
                 "UPDATE ai_provider_credentials SET endpoint = $1 WHERE id = $2",
                 ["https://changed-embedding.example/v1", credential_id]
               )

      Postgrex.query!(connection, "COMMIT", [])

      assert_receive {:backfill_result, {:error, :embedding_credential_not_found}}, 2_000

      assert System.get_config("embedding.shadow_migration.manifest.credential_id") == nil
    after
      Postgrex.query(connection, "ROLLBACK", [])

      Postgrex.query!(connection, "DELETE FROM ai_provider_credentials WHERE id = $1", [
        credential_id
      ])

      GenServer.stop(connection)
    end
  end

  test "abort clears failed pre-cutover backfill state and permits credential repair" do
    credential = shadow_credential_fixture()
    assert :ok = ShadowMigration.prepare(@target_dimension)

    Req.Test.stub(Client, fn conn ->
      conn
      |> Plug.Conn.put_status(401)
      |> Req.Test.json(%{"error" => "invalid credential"})
    end)

    assert {:error, {:embedding_http_error, 401}} =
             ShadowMigration.backfill(backfill_opts(credential))

    assert table_exists?("chunks_embedding_shadow")

    assert System.get_config("embedding.shadow_migration.manifest.credential_id") ==
             to_string(credential.id)

    assert {:error, blocked_changeset} =
             System.update_ai_provider_credential(credential, %{api_key: "repaired-key"})

    assert "cannot modify credential used by active embedding migration" in errors_on(
             blocked_changeset
           ).base

    assert function_exported?(ShadowMigration, :abort, 0)
    assert :ok = ShadowMigration.abort()

    refute table_exists?("chunks_embedding_shadow")
    assert System.get_config("embedding.shadow_migration.manifest.credential_id") == nil
    assert System.get_config("embedding.shadow_migration.manifest.provider") == nil
    assert System.get_config("embedding.shadow_migration.manifest.endpoint") == nil
    assert System.get_config("embedding.shadow_migration.manifest.model") == nil
    assert System.get_config("embedding.shadow_migration.manifest.dimension") == nil

    assert {:ok, repaired_credential} =
             System.update_ai_provider_credential(credential, %{api_key: "repaired-key"})

    assert :ok = ShadowMigration.prepare(@target_dimension)

    Req.Test.stub(Client, fn conn ->
      assert Plug.Conn.get_req_header(conn, "authorization") == ["Bearer repaired-key"]

      Req.Test.json(conn, %{
        "data" => [%{"index" => 0, "embedding" => List.duplicate(0.2, @target_dimension)}]
      })
    end)

    assert {:ok, %{processed: 1, remaining: 0}} =
             ShadowMigration.backfill(backfill_opts(repaired_credential))
  end

  test "abort refuses post-cutover state without mutation", %{
    target_credential: target_credential
  } do
    assert :ok = ShadowMigration.prepare(@target_dimension)
    copy_source_to_shadow(@target_dimension)
    set_shadow_manifest(target_credential)

    assert :ok =
             ShadowMigration.cutover(
               credential_id: target_credential.id,
               model: "target-model",
               dimension: @target_dimension
             )

    assert function_exported?(ShadowMigration, :abort, 0)
    assert {:error, :rollback_table_exists} = ShadowMigration.abort()
    assert current_dimension("chunks") == @target_dimension
    assert current_dimension("chunks_embedding_rollback") == @source_dimension

    assert System.get_config("embedding.shadow_migration.manifest.credential_id") ==
             to_string(target_credential.id)
  end

  test "backfill refuses ambiguous provider and endpoint credentials" do
    credential = shadow_credential_fixture()

    _duplicate =
      SystemConfigFixtures.ai_credential_fixture(%{
        provider: credential.provider,
        endpoint: String.trim_trailing(credential.endpoint, "/")
      })

    assert :ok = ShadowMigration.prepare(@target_dimension)

    Req.Test.stub(Client, fn _conn ->
      flunk("ambiguous credentials must fail before an HTTP request")
    end)

    assert {:error, :ambiguous_embedding_credential} =
             ShadowMigration.backfill(backfill_opts(credential))
  end

  defp shadow_credential_fixture do
    SystemConfigFixtures.ai_credential_fixture(%{
      provider: "scaleway",
      endpoint: "https://api.scaleway.example/v1/",
      api_key: "shadow-key"
    })
  end

  defp backfill_opts(credential) do
    [
      provider: "scaleway",
      endpoint: String.trim_trailing(credential.endpoint, "/"),
      model: "target-model",
      dimension: @target_dimension,
      batch_size: 64
    ]
  end

  defp set_shadow_manifest(credential) do
    values = %{
      "embedding.shadow_migration.manifest.credential_id" => credential.id,
      "embedding.shadow_migration.manifest.provider" => credential.provider,
      "embedding.shadow_migration.manifest.endpoint" =>
        String.trim_trailing(credential.endpoint, "/"),
      "embedding.shadow_migration.manifest.model" => "target-model",
      "embedding.shadow_migration.manifest.dimension" => @target_dimension
    }

    Enum.each(values, fn {key, value} ->
      {:ok, _row} = System.set_config(key, value)
    end)
  end

  defp set_embedding_config(credential_id, model, dimension) do
    {:ok, _} = System.set_config("embedding.credential_id", credential_id)
    {:ok, _} = System.set_config("embedding.model", model)
    {:ok, _} = System.set_config("embedding.dimension", dimension)
  end

  defp insert_chunk(table, document_id, content, dimension) do
    vector = Pgvector.HalfVector.new(List.duplicate(0.1, dimension))

    %Postgrex.Result{rows: [[id]]} =
      Repo.query!(
        """
        INSERT INTO #{table}
          (document_id, content, chunk_index, section_path, metadata, embedding, language,
           inserted_at, updated_at)
        VALUES ($1, $2, 0, '{}', '{}', $3::halfvec, 'fr', NOW(), NOW())
        RETURNING id
        """,
        [document_id, content, vector]
      )

    id
  end

  defp copy_source_to_shadow(dimension) do
    vector = Pgvector.HalfVector.new(List.duplicate(0.2, dimension))

    Repo.query!(
      """
      INSERT INTO chunks_embedding_shadow
        (id, document_id, content, chunk_index, section_path, metadata, embedding, language,
         inserted_at, updated_at)
      SELECT id, document_id, content, chunk_index, section_path, metadata, $1::halfvec,
             language, inserted_at, updated_at
      FROM chunks
      """,
      [vector]
    )
  end

  defp reset_shadow! do
    Repo.query!("DROP TABLE IF EXISTS chunks_embedding_shadow CASCADE", [])
    :ok = ShadowMigration.prepare(@target_dimension)
  end

  defp create_shadow_without_migration_lock(target_dimension) do
    Repo.query!(
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

    Repo.query!(
      """
      CREATE INDEX chunks_embedding_shadow_document_id_index
      ON chunks_embedding_shadow (document_id)
      """,
      []
    )

    Repo.query!(
      """
      CREATE INDEX chunks_embedding_shadow_content_tsv_idx
      ON chunks_embedding_shadow USING gin (content_tsv)
      """,
      []
    )

    Repo.query!(
      """
      CREATE INDEX chunks_embedding_shadow_embedding_idx
      ON chunks_embedding_shadow
      USING hnsw (embedding halfvec_l2_ops)
      WITH (m = 16, ef_construction = 64)
      """,
      []
    )
  end

  defp current_dimension(table) do
    %Postgrex.Result{rows: [[type]]} =
      Repo.query!(
        """
        SELECT format_type(attribute.atttypid, attribute.atttypmod)
        FROM pg_attribute AS attribute
        WHERE attribute.attrelid = to_regclass($1::text)
          AND attribute.attname = 'embedding'
          AND NOT attribute.attisdropped
        """,
        [table]
      )

    [_, dimension] = Regex.run(~r/^halfvec\((\d+)\)$/, type)
    String.to_integer(dimension)
  end

  defp generated_expression(table) do
    %Postgrex.Result{rows: [[expression]]} =
      Repo.query!(
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

    expression
  end

  defp wait_for_advisory_waiters(connection, expected_count, attempts \\ 100)

  defp wait_for_advisory_waiters(_connection, _expected_count, 0) do
    flunk("timed out waiting for migration-lock waiter")
  end

  defp wait_for_advisory_waiters(connection, expected_count, attempts) do
    %Postgrex.Result{rows: [[waiter_count]]} =
      Postgrex.query!(
        connection,
        "SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted",
        []
      )

    if waiter_count >= expected_count do
      :ok
    else
      Process.sleep(10)
      wait_for_advisory_waiters(connection, expected_count, attempts - 1)
    end
  end

  defp direct_postgrex_options do
    Keyword.take(Repo.config(), [
      :hostname,
      :port,
      :username,
      :password,
      :database,
      :socket_dir,
      :ssl,
      :ssl_opts
    ])
  end

  defp table_exists?(table) do
    %Postgrex.Result{rows: [[exists?]]} =
      Repo.query!("SELECT to_regclass($1) IS NOT NULL", [table])

    exists?
  end
end
