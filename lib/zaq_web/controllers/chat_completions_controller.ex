defmodule ZaqWeb.ChatCompletionsController do
  @moduledoc """
  OpenAI-compatible **Chat Completions** endpoint for the `:chat` channel.

      POST /v1/chat/completions
        {model, messages:[{role,content}], stream, user, conversation_id}
        -> data: {"choices":[{"delta":{"role":"assistant"}}]}
        -> data: {"choices":[{"delta":{"content":"…"}}]}
        -> data: {"choices":[{"delta":{},"finish_reason":"stop"}]}
        -> data: {"choices":[{"delta":{}}],"zaq_sources":[…]}       (citations)
        -> data: [DONE]

  A stock OpenAI Chat Completions wire, so any OpenAI-compatible client consumes
  it. Chat Completions has no native source channel, so retrieved citations ride
  a final frame under the non-standard `zaq_sources` key — OpenAI-only clients
  ignore the extra field.

  ## No client-resent history

  The caller sends ONLY the new user message. Prior turns are rehydrated
  server-side: the run is scoped to `chat:conv:<conversation_id>`, which cold-
  starts the agent with that conversation's history (`Zaq.Agent.HistoryLoader`),
  and each turn is persisted (`Zaq.Engine.Conversations.persist_from_incoming/2`)
  so the next cold start sees it.

  ## Security

  - Bearer auth (`ZAQ_CHAT_TOKEN`, constant-time, fail-closed) proves the caller
    is a trusted backend.
  - `user` (the caller's authenticated Supabase user id) + `conversation_id`
    gate access: a conversation is read/appended ONLY when its `channel_user_id`
    matches `user` and its `channel_type` is `"chat"`. This is the IDOR guard —
    history loads by `conversation_id`, so an unowned id must never resolve.
  - Permission scoping runs with `person_id: nil` and `skip_permissions: false`,
    so retrieval surfaces ONLY `"public"`-tagged documents. The anonymous
    transport never blanket-bypasses document permissions.
  """

  use ZaqWeb, :controller

  require Logger

  alias Zaq.Agent.{AnsweringRun, Executor}
  alias Zaq.Engine.Conversations
  alias Zaq.Engine.Conversations.Conversation
  alias Zaq.Engine.Messages.Incoming

  @max_messages 200
  @max_sources 8
  @max_iter_sentinel "Maximum iterations reached"

  # ---------------------------------------------------------------------------
  # POST /v1/chat/completions
  # ---------------------------------------------------------------------------

  def completions(conn, params) do
    with {:ok, question} <- fetch_question(params),
         {:ok, user_id} <- fetch_required(params, "user", "user (owner id) is required"),
         {:ok, convo_id} <-
           fetch_required(params, "conversation_id", "conversation_id is required"),
         {:ok, _conv} <- ensure_owned_conversation(convo_id, user_id) do
      incoming = build_incoming(question, convo_id, user_id)
      # Grounding (an optional OpenAI `system` message) frames THIS run only —
      # passed as the run question so it is injected into retrieval but never
      # persisted (the stored user turn stays the clean question).
      run(conn, incoming, params, with_system(system_content(params), question))
    else
      {:error, status, message} -> json_error(conn, status, message)
    end
  end

  defp run(conn, incoming, params, run_question) do
    stream? = stream?(params)
    id = "chatcmpl-" <> Integer.to_string(System.unique_integer([:positive]))
    model = fetch(params, "model") || "zaq-chat"

    case Executor.stream(incoming,
           question: run_question,
           source_filter: parse_source_filter(params)
         ) do
      {:ok, %{events: events}} ->
        conn = if stream?, do: start_sse(conn), else: conn

        %{
          conn: conn,
          id: id,
          created: created_ts(),
          model: model,
          stream?: stream?,
          buffer: [],
          error: nil,
          seen: MapSet.new(),
          sources: [],
          role_sent: false
        }
        |> fold(events)
        |> respond(incoming)

      {:error, reason} ->
        if stream? do
          conn
          |> start_sse()
          |> emit(stream_error(id, model, created_ts(), reason))
          |> sse_done()
        else
          json_error(conn, 502, error_message(reason))
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Event fold — classify jido events, buffer text, collect citations.
  # ---------------------------------------------------------------------------

  defp fold(acc, events) do
    Enum.reduce_while(events, acc, fn event, acc ->
      acc = collect_sources(acc, AnsweringRun.extract_chunks(event))

      case AnsweringRun.classify_event(event) do
        {:done, final} -> {:halt, backfill(acc, final)}
        {:error, reason} -> {:halt, %{acc | error: reason}}
        :ignore -> {:cont, acc}
      end
    end)
  end

  # The authoritative request result is sent as one OpenAI delta. Intermediate
  # ReAct calls also emit content deltas and must not be merged into the answer.
  defp push_delta(%{stream?: true} = acc, delta) do
    acc = maybe_send_role(acc)

    case emit_status(acc.conn, chunk(acc, %{content: delta}, nil)) do
      {conn, :ok} -> %{acc | conn: conn, buffer: [delta | acc.buffer]}
      {conn, :closed} -> %{acc | conn: conn, error: :client_gone}
    end
  end

  defp push_delta(%{stream?: false} = acc, delta),
    do: %{acc | buffer: [delta | acc.buffer]}

  defp maybe_send_role(%{role_sent: true} = acc), do: acc

  defp maybe_send_role(acc),
    do: %{acc | conn: emit(acc.conn, chunk(acc, %{role: "assistant"}, nil)), role_sent: true}

  # The request-completed result is authoritative. The "max iterations"
  # sentinel is not a real answer: surface it as an error rather than a
  # fabricated acknowledgement.
  defp backfill(%{buffer: [_ | _]} = acc, _final), do: acc

  defp backfill(acc, final) when is_binary(final) do
    cond do
      String.contains?(final, @max_iter_sentinel) -> %{acc | error: :max_iterations_reached}
      String.trim(final) == "" -> %{acc | error: :empty_answer}
      true -> push_delta(acc, final)
    end
  end

  defp backfill(acc, _final), do: %{acc | error: :empty_answer}

  # ---------------------------------------------------------------------------
  # Response — stream terminal frames, or a single blocking completion.
  # ---------------------------------------------------------------------------

  defp respond(%{stream?: true, error: nil} = acc, incoming) do
    persist(incoming, acc)

    conn = emit(acc.conn, chunk(acc, %{}, "stop"))
    conn = if acc.sources == [], do: conn, else: emit(conn, sources_frame(acc))
    sse_done(conn)
  end

  defp respond(%{stream?: true, error: reason} = acc, _incoming) do
    acc.conn
    |> emit(stream_error(acc.id, acc.model, acc.created, reason))
    |> sse_done()
  end

  defp respond(%{stream?: false, error: nil} = acc, incoming) do
    persist(incoming, acc)
    json(acc.conn, completion(acc, "stop"))
  end

  defp respond(%{stream?: false, error: reason} = acc, _incoming) do
    case assembled(acc) do
      "" -> json_error(acc.conn, 502, error_message(reason))
      _text -> json(acc.conn, completion(acc, "stop"))
    end
  end

  # ---------------------------------------------------------------------------
  # Persistence — user + assistant turn into the owned conversation.
  # ---------------------------------------------------------------------------

  defp persist(incoming, acc) do
    answer = assembled(acc)

    if String.trim(answer) != "" do
      # Citations travel live on the wire; durable source rows need the
      # CitationNormalizer input shape confirmed. Add when history needs sources.
      Conversations.persist_from_incoming(incoming, %{
        answer: answer,
        sources: [],
        confidence_score: nil,
        latency_ms: nil,
        prompt_tokens: nil,
        completion_tokens: nil,
        total_tokens: nil
      })
    end
  rescue
    error -> Logger.warning("chat persist failed: #{inspect(error)}")
  end

  # ---------------------------------------------------------------------------
  # Ownership gate (IDOR guard) + conversation lifecycle.
  # ---------------------------------------------------------------------------

  defp ensure_owned_conversation(convo_id, user_id) do
    # Validate the UUID up front so a malformed id is a clean 400 regardless of
    # which cast exception the Repo would raise (the rescue below stays as a
    # belt-and-suspenders guard).
    case Ecto.UUID.cast(convo_id) do
      {:ok, valid_id} -> gate_conversation(valid_id, user_id)
      :error -> {:error, 400, "invalid conversation_id"}
    end
  end

  defp gate_conversation(convo_id, user_id) do
    case Conversations.get_conversation(convo_id) do
      %Conversation{channel_user_id: ^user_id, channel_type: "chat"} = conv ->
        {:ok, conv}

      %Conversation{} ->
        {:error, 403, "conversation does not belong to user"}

      nil ->
        open_conversation(convo_id, user_id)
    end
  rescue
    Ecto.Query.CastError -> {:error, 400, "invalid conversation_id"}
  end

  defp open_conversation(convo_id, user_id) do
    case Conversations.create_chat_conversation(convo_id, user_id) do
      {:ok, %Conversation{} = conv} ->
        {:ok, conv}

      {:error, _changeset} ->
        # Lost a create race (or the id is now taken): re-fetch and re-gate.
        case Conversations.get_conversation(convo_id) do
          %Conversation{channel_user_id: ^user_id, channel_type: "chat"} = conv -> {:ok, conv}
          %Conversation{} -> {:error, 403, "conversation does not belong to user"}
          nil -> {:error, 409, "could not open conversation"}
        end
    end
  end

  defp build_incoming(question, convo_id, user_id) do
    Incoming.new(%{
      content: question,
      channel_id: convo_id,
      author_id: user_id,
      provider: :chat,
      metadata: %{conversation_id: convo_id}
    })
  end

  # ---------------------------------------------------------------------------
  # Citations — unique (document, page) rows captured from the agent's
  # retrievals, capped at @max_sources.
  # ---------------------------------------------------------------------------

  defp collect_sources(%{sources: sources} = acc, _chunks) when length(sources) >= @max_sources,
    do: acc

  defp collect_sources(acc, chunks) do
    Enum.reduce(chunks, acc, &maybe_add_source(&2, source_from_chunk(&1)))
  end

  defp maybe_add_source(acc, nil), do: acc

  defp maybe_add_source(acc, {key, did, src, page}) do
    if length(acc.sources) >= @max_sources or MapSet.member?(acc.seen, key),
      do: acc,
      else: add_source(acc, key, did, src, page)
  end

  defp source_from_chunk(chunk) do
    source = Map.get(chunk, "source") || Map.get(chunk, :source)
    document_id = Map.get(chunk, "document_id") || Map.get(chunk, :document_id)
    page = Map.get(chunk, "page") || Map.get(chunk, :page) || 1

    if is_binary(source) and not is_nil(document_id),
      do: {{document_id, page}, document_id, source, page},
      else: nil
  end

  defp add_source(acc, key, did, src, page) do
    %{
      acc
      | seen: MapSet.put(acc.seen, key),
        sources: acc.sources ++ [%{document_id: did, source: src, page: page}]
    }
  end

  defp sources_frame(acc) do
    acc
    |> chunk(%{}, nil)
    |> Map.put(:zaq_sources, sources_payload(acc.sources))
  end

  defp sources_payload(sources) do
    Enum.map(sources, fn %{document_id: did, source: src, page: page} ->
      %{
        sourceId: did,
        # `?page=` keeps each (doc, page) row distinct and opens at that page.
        url: "/chat/documents/#{did}?page=#{page}",
        title: src,
        page: page
      }
    end)
  end

  # ---------------------------------------------------------------------------
  # OpenAI wire shapes.
  # ---------------------------------------------------------------------------

  defp chunk(acc, delta, finish_reason) do
    %{
      id: acc.id,
      object: "chat.completion.chunk",
      created: acc.created,
      model: acc.model,
      choices: [%{index: 0, delta: delta, finish_reason: finish_reason}]
    }
  end

  defp completion(acc, finish_reason) do
    %{
      id: acc.id,
      object: "chat.completion",
      created: acc.created,
      model: acc.model,
      choices: [
        %{
          index: 0,
          message: %{role: "assistant", content: assembled(acc)},
          finish_reason: finish_reason
        }
      ],
      zaq_sources: sources_payload(acc.sources)
    }
  end

  defp stream_error(id, model, created, reason) do
    %{
      id: id,
      object: "chat.completion.chunk",
      created: created,
      model: model,
      error: %{message: error_message(reason), type: "server_error"}
    }
  end

  defp assembled(%{buffer: buffer}), do: buffer |> Enum.reverse() |> Enum.join("")

  defp created_ts, do: System.system_time(:second)

  # ---------------------------------------------------------------------------
  # Request parsing.
  # ---------------------------------------------------------------------------

  defp fetch_question(params) do
    messages = fetch(params, "messages") || []

    cond do
      not is_list(messages) ->
        {:error, 400, "messages must be an array"}

      length(messages) > @max_messages ->
        {:error, 413, "too many messages (max #{@max_messages})"}

      true ->
        case last_user_content(messages) do
          text when is_binary(text) and text != "" -> {:ok, text}
          _ -> {:error, 400, "no user message provided"}
        end
    end
  end

  defp fetch_required(params, key, message) do
    case fetch(params, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, 400, message}
    end
  end

  # First `system`-role message → per-run framing (OpenAI-standard). Prepended to
  # the run question (see `run/4`), never persisted.
  defp system_content(params) do
    (fetch(params, "messages") || [])
    |> Enum.find_value(fn msg ->
      if fetch(msg, "role") == "system", do: message_text(fetch(msg, "content")), else: nil
    end)
  end

  defp with_system(system, question)
       when is_binary(system) and system != "" and is_binary(question),
       do: system <> "\n\n" <> question

  defp with_system(_system, question), do: question

  # Optional `source_filter` (a ZAQ extension): a list of source prefixes the
  # retrieval is restricted to. Accepts a list or single string; empty/absent →
  # nil (unrestricted). Enforces the councils per-commune isolation invariant.
  defp parse_source_filter(params) do
    case fetch(params, "source_filter") do
      list when is_list(list) ->
        case Enum.filter(list, &is_binary/1) do
          [] -> nil
          filtered -> filtered
        end

      value when is_binary(value) and value != "" ->
        [value]

      _ ->
        nil
    end
  end

  defp stream?(params), do: fetch(params, "stream") == true

  defp last_user_content(messages) when is_list(messages) do
    messages
    |> Enum.reverse()
    |> Enum.find_value(fn msg ->
      if fetch(msg, "role") == "user", do: message_text(fetch(msg, "content")), else: nil
    end)
  end

  defp message_text(text) when is_binary(text), do: text

  defp message_text(parts) when is_list(parts) do
    Enum.map_join(parts, "", fn part -> fetch(part, "text") || "" end)
  end

  defp message_text(_), do: nil

  # ---------------------------------------------------------------------------
  # SSE writer.
  # ---------------------------------------------------------------------------

  defp start_sse(conn) do
    conn
    |> put_resp_header("content-type", "text/event-stream")
    |> put_resp_header("cache-control", "no-cache")
    |> put_resp_header("connection", "keep-alive")
    |> send_chunked(200)
  end

  defp emit(conn, event) when is_map(event) do
    {conn, _status} = emit_status(conn, event)
    conn
  end

  # Emits a frame and reports whether the socket is still open, so the fold can
  # halt when the client has disconnected (`{:error, :closed}` from chunk/2).
  defp emit_status(conn, event) when is_map(event) do
    case chunk_out(conn, "data: #{Jason.encode!(event)}\n\n") do
      {:ok, conn} -> {conn, :ok}
      {:error, _reason} -> {conn, :closed}
    end
  end

  defp sse_done(conn) do
    case chunk_out(conn, "data: [DONE]\n\n") do
      {:ok, conn} -> conn
      {:error, _reason} -> conn
    end
  end

  defp chunk_out(conn, payload), do: Plug.Conn.chunk(conn, payload)

  defp json_error(conn, status, message) do
    conn |> put_status(status) |> json(%{error: %{message: message}})
  end

  defp error_message(:empty_answer), do: "Aucune réponse générée."
  defp error_message(:max_iterations_reached), do: "La recherche n'a pas abouti à une réponse."
  defp error_message(reason) when is_binary(reason), do: reason
  defp error_message(_reason), do: "Une erreur est survenue. Veuillez réessayer."

  defp fetch(map, key) when is_map(map), do: Map.get(map, key)
  defp fetch(_map, _key), do: nil
end
