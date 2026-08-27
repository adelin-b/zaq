defmodule ZaqWeb.ChatDocumentsControllerTest do
  use ZaqWeb.ConnCase, async: false
  use Oban.Testing, repo: Zaq.Repo

  alias Zaq.Ingestion.{Document, IngestJob, IngestWorker}

  @token "test-chat-token"

  defmodule RejectingOban do
    def insert(_changeset), do: {:error, :forced_enqueue_failure}
  end

  defmodule RaisingOban do
    def insert(_changeset), do: raise("forced enqueue exception")
  end

  defmodule RejectingDocumentDelete do
    def get_by_source(source), do: Zaq.Ingestion.Document.get_by_source(source)
    def delete(_document), do: {:error, :forced_document_delete_failure}
  end

  defmodule LegacyIngestionResultRouter do
    def dispatch(%Zaq.Event{} = event) do
      %{event | response: {:ok, %{source: "documents/legacy.pdf", jobs: 1}}}
    end
  end

  setup %{conn: conn} do
    previous_token = System.get_env("ZAQ_CHAT_TOKEN")
    previous_ingestion = Application.get_env(:zaq, Zaq.Ingestion)
    root = Path.join(System.tmp_dir!(), "zaq_chat_documents_#{System.unique_integer()}")
    File.mkdir_p!(root)
    System.put_env("ZAQ_CHAT_TOKEN", @token)
    Application.put_env(:zaq, Zaq.Ingestion, volumes: %{"documents" => root})

    on_exit(fn ->
      restore_env("ZAQ_CHAT_TOKEN", previous_token)
      Application.put_env(:zaq, Zaq.Ingestion, previous_ingestion || [])
      File.rm_rf!(root)
    end)

    conn = put_req_header(conn, "authorization", "Bearer #{@token}")
    {:ok, conn: conn, root: root}
  end

  test "lists and shows only public source documents", %{conn: conn} do
    {:ok, public} =
      Document.create(%{
        source: "documents/pv-public.pdf",
        title: "Public PV",
        content: "# Public",
        tags: ["public"],
        metadata: %{"summary" => "Résumé", "suggestions" => ["Question ?"]}
      })

    {:ok, _private} =
      Document.create(%{source: "documents/pv-private.pdf", title: "Private PV"})

    list = conn |> get("/chat/documents?prefix=documents/pv-") |> json_response(200)
    assert [%{"id" => id, "summary" => "Résumé"}] = list["documents"]
    assert id == public.id

    shown = conn |> get("/chat/documents/#{public.id}") |> json_response(200)
    assert shown["content"] == "# Public"
    assert shown["suggestions"] == ["Question ?"]
  end

  test "streams a public PDF inline", %{conn: conn, root: root} do
    {:ok, document} =
      Document.create(%{source: "documents/pv.pdf", title: "PV", tags: ["public"]})

    File.write!(Path.join(root, "pv.pdf"), "%PDF-test")
    response = get(conn, "/chat/documents/#{document.id}/file")

    assert response(response, 200) == "%PDF-test"
    assert get_resp_header(response, "content-disposition") == [~s(inline; filename="pv.pdf")]
    assert List.first(get_resp_header(response, "content-type")) =~ "application/pdf"
  end

  test "shows linked sidecar content while keeping the PDF original and sidecar private", %{
    conn: conn,
    root: root
  } do
    {:ok, parent} =
      Document.create(%{
        source: "documents/pv.pdf",
        content: "# Extracted PV",
        tags: ["public"],
        metadata: %{"sidecar_source" => "documents/PV_augmente_2026-01-22.md"}
      })

    {:ok, sidecar} =
      Document.create(%{
        source: "documents/PV_augmente_2026-01-22.md",
        content: "# PV augmenté",
        content_type: "markdown",
        metadata: %{"source_document_source" => parent.source}
      })

    shown = conn |> get("/chat/documents/#{parent.id}") |> json_response(200)
    assert shown["content"] == "# PV augmenté"
    assert shown["content_type"] == "markdown"

    File.write!(Path.join(root, "pv.pdf"), "%PDF-original")
    assert conn |> get("/chat/documents/#{parent.id}/file") |> response(200) == "%PDF-original"

    assert conn |> get("/chat/documents/#{sidecar.id}") |> json_response(404)
  end

  test "does not expose a private document or file", %{conn: conn, root: root} do
    {:ok, document} = Document.create(%{source: "documents/private.pdf"})
    File.write!(Path.join(root, "private.pdf"), "%PDF-private")

    assert conn |> get("/chat/documents/#{document.id}") |> json_response(404)
    assert conn |> get("/chat/documents/#{document.id}/file") |> json_response(404)
  end

  test "create writes the file, marks the folder public and enqueues ingestion",
       %{conn: conn, root: root} do
    body = %{
      "path" => "PV CM Test - 00000/pv-2026-01-15.pdf",
      "content_base64" => Base.encode64("%PDF-pushed"),
      "public" => true,
      "volume" => "documents"
    }

    Oban.Testing.with_testing_mode(:manual, fn ->
      resp = conn |> post("/chat/documents", body) |> json_response(202)
      assert resp["source"] == "documents/PV CM Test - 00000/pv-2026-01-15.pdf"
      assert resp["created"] == true

      assert %{id: job_id, status: "pending"} =
               Zaq.Repo.get_by(IngestJob,
                 file_path: "PV CM Test - 00000/pv-2026-01-15.pdf"
               )

      assert [_job] = all_enqueued(worker: IngestWorker, args: %{"job_id" => job_id})
    end)

    assert File.read!(Path.join(root, "PV CM Test - 00000/pv-2026-01-15.pdf")) == "%PDF-pushed"

    assert %{tags: ["public"]} =
             Zaq.Repo.get_by(Zaq.Ingestion.FolderSetting,
               volume_name: "documents",
               folder_path: "PV CM Test - 00000"
             )
  end

  test "create treats a rolling legacy ingestion result as a created upload", %{conn: conn} do
    previous_router = Application.get_env(:zaq, :chat_documents_node_router_module)
    Application.put_env(:zaq, :chat_documents_node_router_module, LegacyIngestionResultRouter)

    on_exit(fn ->
      if previous_router,
        do: Application.put_env(:zaq, :chat_documents_node_router_module, previous_router),
        else: Application.delete_env(:zaq, :chat_documents_node_router_module)
    end)

    response =
      conn
      |> post("/chat/documents", %{
        "path" => "legacy.pdf",
        "content_base64" => Base.encode64("%PDF-legacy"),
        "volume" => "documents"
      })
      |> json_response(202)

    assert response == %{
             "created" => true,
             "jobs" => 1,
             "source" => "documents/legacy.pdf"
           }
  end

  test "create reports enqueue failure and settles the tracking row", %{conn: conn} do
    previous_oban = Application.get_env(:zaq, :ingestion_oban_module)
    Application.put_env(:zaq, :ingestion_oban_module, RejectingOban)

    on_exit(fn ->
      if previous_oban,
        do: Application.put_env(:zaq, :ingestion_oban_module, previous_oban),
        else: Application.delete_env(:zaq, :ingestion_oban_module)
    end)

    body = %{
      "path" => "failed-enqueue.pdf",
      "content_base64" => Base.encode64("%PDF-pushed"),
      "volume" => "documents"
    }

    response = conn |> post("/chat/documents", body) |> json_response(502)
    assert response["error"]["message"] == "ingestion failed: :enqueue_failed"

    assert %{status: "failed", error: "Failed to enqueue ingestion job."} =
             Zaq.Repo.get_by(IngestJob, file_path: "failed-enqueue.pdf")
  end

  test "create reports an enqueue exception and settles the tracking row", %{conn: conn} do
    previous_oban = Application.get_env(:zaq, :ingestion_oban_module)
    Application.put_env(:zaq, :ingestion_oban_module, RaisingOban)

    on_exit(fn ->
      if previous_oban,
        do: Application.put_env(:zaq, :ingestion_oban_module, previous_oban),
        else: Application.delete_env(:zaq, :ingestion_oban_module)
    end)

    body = %{
      "path" => "raised-enqueue.pdf",
      "content_base64" => Base.encode64("%PDF-pushed"),
      "volume" => "documents"
    }

    response = conn |> post("/chat/documents", body) |> json_response(502)
    assert response["error"]["message"] == "ingestion failed: :enqueue_failed"

    assert %{status: "failed", error: "Failed to enqueue ingestion job."} =
             Zaq.Repo.get_by(IngestJob, file_path: "raised-enqueue.pdf")
  end

  test "delete reports filesystem failure without deleting the document row", %{
    conn: conn,
    root: root
  } do
    rel_path = "locked/pv.pdf"
    source = "documents/#{rel_path}"
    directory = Path.join(root, "locked")
    file = Path.join(root, rel_path)

    File.mkdir_p!(directory)
    File.write!(file, "%PDF-locked")
    {:ok, _document} = Document.create(%{source: source})
    File.chmod!(directory, 0o500)
    on_exit(fn -> File.chmod(directory, 0o700) end)

    response =
      conn
      |> delete("/chat/documents", %{"path" => rel_path, "volume" => "documents"})
      |> json_response(502)

    assert response["error"]["message"] == "delete failed: :file_delete_failed"
    assert File.exists?(file)
    assert %Document{} = Document.get_by_source(source)
  end

  test "delete reports database failure after removing the file", %{conn: conn, root: root} do
    previous_document_module = Application.get_env(:zaq, :ingestion_document_module)
    Application.put_env(:zaq, :ingestion_document_module, RejectingDocumentDelete)

    on_exit(fn ->
      if previous_document_module,
        do: Application.put_env(:zaq, :ingestion_document_module, previous_document_module),
        else: Application.delete_env(:zaq, :ingestion_document_module)
    end)

    rel_path = "database-failure.pdf"
    source = "documents/#{rel_path}"
    file = Path.join(root, rel_path)

    File.write!(file, "%PDF-database-failure")
    {:ok, _document} = Document.create(%{source: source})

    response =
      conn
      |> delete("/chat/documents", %{"path" => rel_path, "volume" => "documents"})
      |> json_response(502)

    assert response["error"]["message"] == "delete failed: :document_delete_failed"
    refute File.exists?(file)
    assert %Document{} = Document.get_by_source(source)
  end

  test "delete is idempotent when file and document row are absent", %{conn: conn} do
    response =
      conn
      |> delete("/chat/documents", %{
        "path" => "already-absent.pdf",
        "volume" => "documents"
      })

    assert response(response, 204) == ""
  end

  test "delete removes a document row when the file is already absent", %{conn: conn} do
    rel_path = "file-already-absent.pdf"
    source = "documents/#{rel_path}"
    {:ok, _document} = Document.create(%{source: source})

    response =
      conn
      |> delete("/chat/documents", %{"path" => rel_path, "volume" => "documents"})

    assert response(response, 204) == ""
    assert Document.get_by_source(source) == nil
  end

  test "create-only preserves an existing file without ingestion side effects", %{
    conn: conn,
    root: root
  } do
    rel_path = "protected/existing-sidecar.md"
    file = Path.join(root, rel_path)
    File.mkdir_p!(Path.dirname(file))
    File.write!(file, "# Healthy existing sidecar")

    body = %{
      "path" => rel_path,
      "content_base64" => Base.encode64("# Healthy existing sidecar"),
      "public" => true,
      "ingest" => true,
      "create_only" => true,
      "volume" => "documents"
    }

    response = conn |> post("/chat/documents", body) |> json_response(200)

    assert response == %{
             "created" => false,
             "jobs" => 0,
             "source" => "documents/#{rel_path}"
           }

    assert File.read!(file) == "# Healthy existing sidecar"
    assert Zaq.Repo.get_by(IngestJob, file_path: rel_path) == nil

    assert Zaq.Repo.get_by(Zaq.Ingestion.FolderSetting,
             volume_name: "documents",
             folder_path: "protected"
           ) == nil
  end

  test "create-only rejects different content without replacing the existing file", %{
    conn: conn,
    root: root
  } do
    rel_path = "protected/conflicting-sidecar.md"
    file = Path.join(root, rel_path)
    File.mkdir_p!(Path.dirname(file))
    File.write!(file, "# Healthy existing sidecar")

    response =
      conn
      |> post("/chat/documents", %{
        "path" => rel_path,
        "content_base64" => Base.encode64("# Different sidecar"),
        "create_only" => true,
        "volume" => "documents"
      })
      |> json_response(409)

    assert response["error"]["message"] == "file already exists with different content"
    assert File.read!(file) == "# Healthy existing sidecar"
  end

  test "create-only concurrent writers create exactly one file", %{root: root} do
    rel_path = "concurrent-sidecar.md"
    content = "# Shared sidecar"

    results =
      [content, content]
      |> Task.async_stream(
        fn content ->
          Zaq.Ingestion.ingest_chat_document(%{
            path: rel_path,
            content: content,
            public: false,
            ingest: false,
            create_only: true,
            volume: "documents"
          })
        end,
        max_concurrency: 2,
        ordered: false
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.sort(Enum.map(results, fn {:ok, result} -> result.created end)) == [false, true]
    assert Enum.all?(results, fn {:ok, result} -> result.jobs == 0 end)
    assert File.read!(Path.join(root, rel_path)) == content
  end

  test "create rejects traversal, bad base64 and missing fields", %{conn: conn} do
    base = %{"content_base64" => Base.encode64("x"), "volume" => "documents"}

    assert conn
           |> post("/chat/documents", Map.put(base, "path", "../escape.pdf"))
           |> json_response(400)

    assert conn
           |> post("/chat/documents", %{"path" => "a.pdf", "content_base64" => "%%%"})
           |> json_response(400)

    assert conn |> post("/chat/documents", %{}) |> json_response(400)
  end

  defp restore_env(name, nil), do: System.delete_env(name)
  defp restore_env(name, value), do: System.put_env(name, value)
end
