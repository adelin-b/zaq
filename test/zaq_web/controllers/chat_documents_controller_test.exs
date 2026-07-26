defmodule ZaqWeb.ChatDocumentsControllerTest do
  use ZaqWeb.ConnCase, async: false

  alias Zaq.Ingestion.Document

  @token "test-chat-token"

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

    resp = conn |> post("/chat/documents", body) |> json_response(202)
    assert resp["source"] == "documents/PV CM Test - 00000/pv-2026-01-15.pdf"

    assert File.read!(Path.join(root, "PV CM Test - 00000/pv-2026-01-15.pdf")) == "%PDF-pushed"

    assert %{tags: ["public"]} =
             Zaq.Repo.get_by(Zaq.Ingestion.FolderSetting,
               volume_name: "documents",
               folder_path: "PV CM Test - 00000"
             )
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
