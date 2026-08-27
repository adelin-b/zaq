defmodule ZaqWeb.ChatDocumentsSymlinkSecurityTest do
  use ZaqWeb.ConnCase, async: false

  alias Zaq.Ingestion.Document

  @token "symlink-security-token"

  setup %{conn: conn} do
    previous_token = System.get_env("ZAQ_CHAT_TOKEN")
    previous_ingestion = Application.get_env(:zaq, Zaq.Ingestion)
    root = Path.join(System.tmp_dir!(), "zaq_symlink_root_#{System.unique_integer()}")
    outside = Path.join(System.tmp_dir!(), "zaq_symlink_outside_#{System.unique_integer()}")
    File.mkdir_p!(root)
    File.mkdir_p!(outside)
    File.write!(Path.join(outside, "secret.pdf"), "%PDF-outside")
    File.ln_s!(outside, Path.join(root, "escape"))
    System.put_env("ZAQ_CHAT_TOKEN", @token)
    Application.put_env(:zaq, Zaq.Ingestion, volumes: %{"documents" => root})

    on_exit(fn ->
      restore_env("ZAQ_CHAT_TOKEN", previous_token)
      Application.put_env(:zaq, Zaq.Ingestion, previous_ingestion || [])
      File.rm_rf!(root)
      File.rm_rf!(outside)
    end)

    {:ok, document} =
      Document.create(%{
        source: "documents/escape/secret.pdf",
        title: "Outside PDF",
        tags: ["public"]
      })

    conn = put_req_header(conn, "authorization", "Bearer #{@token}")
    %{conn: conn, document: document}
  end

  test "file endpoint does not serve through an in-volume symlink", %{
    conn: conn,
    document: document
  } do
    response = get(conn, "/chat/documents/#{document.id}/file")

    assert json_response(response, 404)["error"]["message"] == "document file not found"
  end

  defp restore_env(name, nil), do: System.delete_env(name)
  defp restore_env(name, value), do: System.put_env(name, value)
end
