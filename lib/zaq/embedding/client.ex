defmodule Zaq.Embedding.Client do
  @moduledoc """
  Generic OpenAI-compatible embedding HTTP client.

  Posts to any `/embeddings` endpoint that follows the OpenAI API format.
  Works with Scaleway, OpenAI, Ollama, vLLM, LocalAI, and any other
  compatible provider.

  Uses `Req` for HTTP (already a ZAQ dependency).

  ## Configuration

  Embedding settings are managed via the back-office UI at `/bo/system-config`
  and persisted in the database. All values are read directly from the
  database via `Zaq.System.get_embedding_config/0`.

  ## Testing

  In `config/test.exs`, configure Req.Test stubbing:

      config :zaq, Zaq.Embedding.Client,
        req_options: [plug: {Req.Test, Zaq.Embedding.Client}]

  Then in tests, use `Req.Test.stub/2` to mock responses.
  """

  require Logger

  @unix_epoch_floor 1_000_000_000

  @doc """
  Generates an embedding vector for the given text.

  ## Options

    * `:config` — use an explicit embedding config for this call
    * `:model` — override the selected config's model for this call

  ## Examples

      iex> Zaq.Embedding.Client.embed("Hello world")
      {:ok, [0.123, -0.456, ...]}

      iex> Zaq.Embedding.Client.embed("Hello", model: "nomic-embed-text")
      {:ok, [0.789, ...]}
  """
  @spec embed(String.t(), keyword()) :: {:ok, [float()]} | {:error, term()}
  def embed(text, opts \\ []) when is_binary(text) do
    case request_embeddings(text, opts) do
      {:ok, %{"data" => data}} ->
        validate_scalar_response(data)

      {:ok, _response_body} ->
        {:error, :invalid_embedding_response}

      {:error, _reason} = error ->
        error
    end
  end

  @doc """
  Generates embeddings for a non-empty batch of texts in one request.

  Response items are validated and reordered by their OpenAI-compatible
  `index` field before vectors are returned.
  """
  @spec embed_many([String.t()], keyword()) :: {:ok, [[float()]]} | {:error, term()}
  def embed_many(texts, opts \\ [])

  def embed_many([], _opts), do: {:error, "Embedding batch must not be empty"}

  def embed_many(texts, opts) when is_list(texts) do
    if Enum.all?(texts, &is_binary/1) do
      case request_embeddings(texts, opts) do
        {:ok, %{"data" => data}} -> validate_batch_response(data, length(texts))
        {:ok, _response_body} -> invalid_embedding_response()
        {:error, _reason} = error -> error
      end
    else
      {:error, "Embedding batch inputs must be strings"}
    end
  end

  @doc """
  Returns the configured embedding dimension.
  Used by Ecto migrations and vector operations.
  """
  @spec dimension() :: pos_integer()
  def dimension, do: Zaq.System.get_embedding_config().dimension

  @doc "Returns the configured embedding endpoint."
  def endpoint, do: Zaq.System.get_embedding_config().endpoint

  @doc "Returns the configured embedding API key."
  def api_key, do: Zaq.System.get_embedding_config().api_key || ""

  @doc "Returns the configured embedding model."
  def model, do: Zaq.System.get_embedding_config().model

  # -- Private --

  defp request_embeddings(input, opts) do
    cfg = Keyword.get_lazy(opts, :config, &Zaq.System.get_embedding_config/0)
    model = Keyword.get(opts, :model, cfg.model)

    case embedding_url(cfg.endpoint) do
      {:ok, url} ->
        headers =
          if cfg.api_key != nil and cfg.api_key != "" do
            [{"authorization", "Bearer #{cfg.api_key}"}]
          else
            []
          end

        req_opts =
          [
            url: url,
            json: %{model: model, input: input},
            headers: headers,
            receive_timeout: 60_000
          ]
          |> Keyword.merge(req_options())

        case Req.post(req_opts) do
          {:ok, %Req.Response{status: 200, body: response_body}} ->
            {:ok, response_body}

          {:ok, %Req.Response{status: 429, headers: response_headers}} ->
            delay_seconds = rate_limit_delay_seconds(response_headers)
            Logger.warning("Embedding API rate limited (429). Retrying in #{delay_seconds}s")
            {:error, {:rate_limited, delay_seconds, %{status: 429}}}

          {:ok, %Req.Response{status: status}} ->
            Logger.error("Embedding API request failed with status #{status}")
            {:error, {:embedding_http_error, status}}

          {:error, _reason} ->
            embedding_transport_error()
        end

      :error ->
        embedding_transport_error()
    end
  end

  defp embedding_url(endpoint) when is_binary(endpoint) do
    normalized_endpoint = endpoint |> String.trim() |> String.trim_trailing("/")
    uri = URI.parse(normalized_endpoint)

    if uri.scheme in ["http", "https"] and is_binary(uri.host) and uri.host != "" do
      {:ok, normalized_endpoint <> "/embeddings"}
    else
      :error
    end
  end

  defp embedding_url(_endpoint), do: :error

  defp embedding_transport_error do
    Logger.error("Embedding HTTP transport failed")
    {:error, :embedding_transport_error}
  end

  defp validate_scalar_response([%{"embedding" => embedding} = item]) do
    if Map.get(item, "index") in [nil, 0] and valid_embedding?(embedding) do
      {:ok, embedding}
    else
      invalid_embedding_response()
    end
  end

  defp validate_scalar_response(_data), do: invalid_embedding_response()

  defp validate_batch_response(data, expected_count) when is_list(data) do
    expected_indices = Enum.to_list(0..(expected_count - 1))

    with true <- length(data) == expected_count,
         true <- Enum.all?(data, &valid_batch_item?/1),
         true <- Enum.sort(Enum.map(data, & &1["index"])) == expected_indices do
      embeddings =
        data
        |> Enum.sort_by(& &1["index"])
        |> Enum.map(& &1["embedding"])

      {:ok, embeddings}
    else
      _ -> invalid_embedding_response()
    end
  end

  defp validate_batch_response(_data, _expected_count), do: invalid_embedding_response()

  defp valid_batch_item?(%{"index" => index, "embedding" => embedding})
       when is_integer(index),
       do: valid_embedding?(embedding)

  defp valid_batch_item?(_item), do: false

  defp valid_embedding?(embedding) when is_list(embedding) and embedding != [],
    do: Enum.all?(embedding, &is_number/1)

  defp valid_embedding?(_embedding), do: false

  defp invalid_embedding_response, do: {:error, :invalid_embedding_response}

  defp rate_limit_delay_seconds(headers) do
    ["retry-after", "ratelimit-reset", "x-ratelimit-reset"]
    |> Enum.find_value(60, fn key ->
      with value when is_binary(value) <- header_value(headers, key),
           {:ok, delay_seconds} <- parse_rate_limit_delay(value) do
        delay_seconds
      else
        _ -> nil
      end
    end)
  end

  defp parse_rate_limit_delay(value) do
    trimmed = String.trim(value)

    case Integer.parse(trimmed) do
      {parsed, ""} when parsed >= 0 -> {:ok, normalize_delay_seconds(parsed)}
      {_parsed, ""} -> :error
      _ -> parse_http_date_delay(trimmed)
    end
  end

  defp normalize_delay_seconds(parsed) when parsed >= @unix_epoch_floor do
    max(parsed - (DateTime.utc_now() |> DateTime.to_unix()), 0)
  end

  defp normalize_delay_seconds(parsed), do: parsed

  defp parse_http_date_delay(""), do: :error

  defp parse_http_date_delay(value) do
    case convert_request_date(value) do
      {:error, _reason} ->
        :error

      :bad_date ->
        :error

      datetime_tuple ->
        delay_seconds =
          datetime_tuple
          |> :calendar.datetime_to_gregorian_seconds()
          |> Kernel.-(:calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}))
          |> Kernel.-(DateTime.utc_now() |> DateTime.to_unix())

        {:ok, max(delay_seconds, 0)}
    end
  end

  defp convert_request_date(value) do
    :httpd_util.convert_request_date(String.to_charlist(value))
  rescue
    FunctionClauseError -> :bad_date
  end

  defp header_value(headers, key) when is_map(headers) do
    headers
    |> Map.get(String.downcase(key))
    |> normalize_header_value()
  end

  defp header_value(headers, key) when is_list(headers) do
    key_downcase = String.downcase(key)

    headers
    |> Enum.find_value(fn
      {header_key, header_value} when is_binary(header_key) ->
        if String.downcase(header_key) == key_downcase do
          normalize_header_value(header_value)
        else
          nil
        end

      _ ->
        nil
    end)
  end

  defp header_value(_headers, _key), do: nil

  defp normalize_header_value([value | _]) when is_binary(value), do: value
  defp normalize_header_value(value) when is_binary(value), do: value
  defp normalize_header_value(_), do: nil

  defp req_options do
    :zaq
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:req_options, [])
  end
end
