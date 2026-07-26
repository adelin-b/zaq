defmodule ZaqWeb.ChatDocumentsController do
  use ZaqWeb, :controller

  alias Zaq.Ingestion
  alias Zaq.Ingestion.{Document, Sidecar}
  alias Zaq.Ingestion.FileExplorer

  def index(conn, %{"prefix" => prefix}) do
    documents =
      prefix
      |> Ingestion.list_public_chat_documents()
      |> Enum.map(&serialize/1)

    json(conn, %{documents: documents})
  end

  def index(conn, _params), do: json(conn, %{documents: []})

  def show(conn, %{"id" => id}) do
    case Ingestion.get_public_chat_document(id) do
      nil -> json_error(conn, 404, "document not found")
      document -> json(conn, serialize(document, true))
    end
  end

  @max_upload_bytes 25 * 1024 * 1024

  @doc """
  Pushes a document into ZAQ ingestion from a trusted backend (bearer-authed).

  Body: `path` (relative to the volume, e.g. `"PV CM X - 63450/pv.pdf"`),
  `content_base64` (file bytes), `public` (bool, folder-level flag),
  `volume` (optional, defaults to the single-volume `"default"`).

  Dispatched to the ingestion role through NodeRouter like every other
  cross-role call; ingestion is async — 202 means the file landed and jobs
  were enqueued.
  """
  def create(conn, %{"path" => path, "content_base64" => encoded} = params)
      when is_binary(path) and is_binary(encoded) do
    with {:ok, content} <- decode_content(encoded),
         {:ok, result} <- dispatch_ingest(path, content, params) do
      conn |> put_status(202) |> json(%{source: result.source, jobs: result.jobs})
    else
      {:error, :invalid_base64} -> json_error(conn, 400, "content_base64 is not valid base64")
      {:error, :too_large} -> json_error(conn, 413, "file exceeds #{@max_upload_bytes} bytes")
      {:error, :path_traversal} -> json_error(conn, 400, "invalid path")
      {:error, :unknown_volume} -> json_error(conn, 400, "unknown volume")
      {:error, reason} -> json_error(conn, 502, "ingestion failed: #{inspect(reason)}")
    end
  end

  def create(conn, _params),
    do: json_error(conn, 400, "path and content_base64 are required")

  defp decode_content(encoded) do
    case Base.decode64(encoded) do
      {:ok, content} when byte_size(content) > @max_upload_bytes -> {:error, :too_large}
      {:ok, content} -> {:ok, content}
      :error -> {:error, :invalid_base64}
    end
  end

  defp dispatch_ingest(path, content, params) do
    attrs = %{
      path: path,
      content: content,
      public: params["public"] == true,
      ingest: params["ingest"] != false,
      volume: params["volume"]
    }

    attrs
    |> Zaq.Event.new(:ingestion, opts: [action: :ingest_chat_document])
    |> Zaq.NodeRouter.dispatch()
    |> Map.fetch!(:response)
    |> case do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_ingest_response, other}}
    end
  end

  def file(conn, %{"id" => id}) do
    with document when not is_nil(document) <- Ingestion.get_public_chat_document(id),
         {:ok, path} <- FileExplorer.resolve_path(document.source),
         true <- File.regular?(path) do
      conn
      |> put_resp_content_type("application/pdf")
      |> put_resp_header("content-disposition", ~s(inline; filename="#{Path.basename(path)}"))
      |> send_file(200, path)
    else
      _ -> json_error(conn, 404, "document file not found")
    end
  end

  defp serialize(document, include_content? \\ false) do
    metadata = document.metadata || %{}

    %{
      id: document.id,
      source: document.source,
      title: document.title,
      summary: metadata["summary"] || metadata[:summary],
      suggestions: metadata["suggestions"] || metadata[:suggestions] || []
    }
    |> maybe_put_content(document, include_content?)
  end

  defp maybe_put_content(payload, document, true) do
    content_document = linked_sidecar(document) || document

    Map.merge(payload, %{
      content: content_document.content,
      content_type: content_document.content_type
    })
  end

  defp maybe_put_content(payload, _document, false), do: payload

  defp linked_sidecar(document) do
    case Sidecar.sidecar_source(document) do
      source when is_binary(source) -> Document.get_by_source(source)
      nil -> nil
    end
  end

  defp json_error(conn, status, message) do
    conn |> put_status(status) |> json(%{error: %{message: message}})
  end
end
