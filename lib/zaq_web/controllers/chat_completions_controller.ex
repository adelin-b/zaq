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

  ## Pipeline routing

  Requests flow through `CommunicationBridge.route_incoming_message/5` →
  `NodeRouter.dispatch/1` like every other channel bridge, so traces, Person
  resolution (`Zaq.People.IdentityResolver`) and conversation persistence come
  from the shared pipeline. The transport is a synchronous HTTP request, so the
  controller subscribes to `Zaq.Channels.ChatBridge.topic/1` BEFORE routing and
  blocks until `ChatBridge.send_reply/2` broadcasts the pipeline `%Outgoing{}`
  back as `{:chat_result, request_id, outgoing}`.

  The SSE wire is preserved for `stream: true` clients, but the answer arrives
  as a single content delta: the shared pipeline returns one final result, and
  intermediate status stages have no surface on the OpenAI wire.

  ## No client-resent history

  The caller sends ONLY the new user message. Prior turns are rehydrated
  server-side: the run is scoped to `chat:conv:<conversation_id>`, which cold-
  starts the agent with that conversation's history (`Zaq.Agent.HistoryLoader`),
  and each turn is persisted by the pipeline's engine hop
  (`Zaq.Engine.Conversations.persist_from_incoming/2`) so the next cold start
  sees it.

  ## Security

  - Bearer auth (`ZAQ_CHAT_TOKEN`, constant-time, fail-closed) proves the caller
    is a trusted backend.
  - `user` (the caller's authenticated Supabase user id) + `conversation_id`
    gate access: a conversation is read/appended ONLY when its `channel_user_id`
    matches `user` and its `channel_type` is `"chat"`. This is the IDOR guard —
    history loads by `conversation_id`, so an unowned id must never resolve.
  - Permission scoping: the pipeline resolves a ZAQ Person from the `chat`
    channel identity (the `user` id). Fresh chat Persons belong to no team, so
    retrieval surfaces only `"public"`-tagged documents — the transport never
    blanket-bypasses document permissions (`skip_permissions` stays false).
  """

  use ZaqWeb, :controller

  alias Zaq.Agent.AnsweringRun
  alias Zaq.Channels.{ChatBridge, CommunicationBridge}
  alias Zaq.Engine.Conversations
  alias Zaq.Engine.Conversations.Conversation
  alias Zaq.Engine.Messages.Outgoing

  @max_messages 200
  @max_sources 8
  @max_iter_sentinel "Maximum iterations reached"
  @default_result_timeout_ms 120_000

  # ---------------------------------------------------------------------------
  # POST /v1/chat/completions
  # ---------------------------------------------------------------------------

  def completions(conn, params) do
    with {:ok, question} <- fetch_question(params),
         {:ok, user_id} <- fetch_required(params, "user", "user (owner id) is required"),
         {:ok, convo_id} <-
           fetch_required(params, "conversation_id", "conversation_id is required"),
         {:ok, _conv} <- ensure_owned_conversation(convo_id, user_id) do
      run(conn, params, question, convo_id, user_id)
    else
      {:error, status, message} -> json_error(conn, status, message)
    end
  end

  defp run(conn, params, question, convo_id, user_id) do
    request_id = Ecto.UUID.generate()
    # Subscribe before routing so the reply broadcast can't win the race.
    :ok = Phoenix.PubSub.subscribe(Zaq.PubSub, ChatBridge.topic(convo_id))

    incoming =
      ChatBridge.to_internal(%{
        content: question,
        conversation_id: convo_id,
        author_id: user_id,
        message_id: request_id,
        source_filter: parse_source_filter(params)
      })

    acc = %{
      conn: conn,
      id: "chatcmpl-" <> Integer.to_string(System.unique_integer([:positive])),
      created: created_ts(),
      model: fetch(params, "model") || "zaq-chat",
      stream?: stream?(params)
    }

    # Grounding (an optional OpenAI `system` message) frames THIS run only —
    # passed as the run question so it is injected into retrieval but never
    # persisted (the stored user turn stays the clean question).
    case route(incoming, with_system(system_content(params), question)) do
      # Sync hop: the pipeline result came straight back.
      %Outgoing{} = outgoing -> respond(acc, outgoing)
      # Async hop: the result arrives via ChatBridge.send_reply/2 over PubSub.
      :ok -> await_result(acc, request_id)
      {:error, reason} -> respond_error(acc, reason)
    end
  end

  defp route(incoming, run_question) do
    CommunicationBridge.route_incoming_message(
      incoming,
      [question: run_question],
      [{:global_default, Zaq.System.get_global_default_agent_id()}],
      actor_from_incoming(incoming),
      node_router: node_router_module()
    )
  end

  defp actor_from_incoming(incoming) do
    %{id: incoming.author_id, name: incoming.author_name, provider: incoming.provider}
  end

  defp await_result(acc, request_id) do
    timeout = Application.get_env(:zaq, :chat_result_timeout_ms, @default_result_timeout_ms)

    receive do
      {:chat_result, ^request_id, %Outgoing{} = outgoing} -> respond(acc, outgoing)
    after
      timeout -> respond_error(acc, :timeout)
    end
  end

  # ---------------------------------------------------------------------------
  # Response — one pipeline result folded onto the OpenAI wire.
  # ---------------------------------------------------------------------------

  defp respond(acc, %Outgoing{} = outgoing) do
    answer = AnsweringRun.clean_answer(outgoing.body)

    case classify(outgoing.metadata, answer) do
      :ok -> deliver(acc, answer, sources_from_outgoing(outgoing))
      {:error, reason} -> respond_error(acc, reason)
    end
  end

  # The pipeline never raises — errors come back flagged on the result. The
  # "max iterations" sentinel is not a real answer: surface it as an error
  # rather than a fabricated acknowledgement.
  defp classify(metadata, answer) do
    cond do
      metadata_get(metadata, :error) == true -> {:error, :pipeline_error}
      String.contains?(answer, @max_iter_sentinel) -> {:error, :max_iterations_reached}
      String.trim(answer) == "" -> {:error, :empty_answer}
      true -> :ok
    end
  end

  defp deliver(%{stream?: true} = acc, answer, sources) do
    conn =
      acc.conn
      |> start_sse()
      |> emit(chunk(acc, %{role: "assistant"}, nil))
      |> emit(chunk(acc, %{content: answer}, nil))
      |> emit(chunk(acc, %{}, "stop"))

    conn = if sources == [], do: conn, else: emit(conn, sources_frame(acc, sources))
    sse_done(conn)
  end

  defp deliver(%{stream?: false} = acc, answer, sources) do
    json(acc.conn, completion(acc, answer, sources, "stop"))
  end

  defp respond_error(%{stream?: true} = acc, reason) do
    acc.conn
    |> start_sse()
    |> emit(stream_error(acc, reason))
    |> sse_done()
  end

  defp respond_error(%{stream?: false} = acc, reason) do
    json_error(acc.conn, 502, error_message(reason))
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

  # ---------------------------------------------------------------------------
  # Citations — unique (document, page) rows from the run's retrieval tool
  # calls (carried on `outgoing.metadata[:tool_calls]`, json_safe-encoded),
  # capped at @max_sources.
  # ---------------------------------------------------------------------------

  defp sources_from_outgoing(%Outgoing{metadata: metadata}) do
    metadata
    |> metadata_get(:tool_calls)
    |> List.wrap()
    |> Enum.flat_map(&tool_call_chunks/1)
    |> Enum.reduce({MapSet.new(), []}, &maybe_add_source(&2, source_from_chunk(&1)))
    |> elem(1)
  end

  # `tool_call["response"]` is the json_safe-encoded raw tool result: either a
  # map (`%{"chunks" => [...]}`) or an encoded ok-tuple (`["ok", %{...}, ...]`).
  defp tool_call_chunks(tool_call) when is_map(tool_call) do
    tool_call |> metadata_get(:response) |> chunks_from_response()
  end

  defp tool_call_chunks(_tool_call), do: []

  defp chunks_from_response(%{} = response) do
    case metadata_get(response, :chunks) do
      chunks when is_list(chunks) -> Enum.filter(chunks, &is_map/1)
      _ -> []
    end
  end

  defp chunks_from_response(["ok" | rest]) do
    case Enum.find(rest, &is_map/1) do
      nil -> []
      inner -> chunks_from_response(inner)
    end
  end

  defp chunks_from_response(_response), do: []

  defp maybe_add_source(acc, nil), do: acc

  defp maybe_add_source({seen, sources} = acc, {key, did, src, page}) do
    if length(sources) >= @max_sources or MapSet.member?(seen, key) do
      acc
    else
      {MapSet.put(seen, key), sources ++ [%{document_id: did, source: src, page: page}]}
    end
  end

  defp source_from_chunk(chunk) do
    source = Map.get(chunk, "source") || Map.get(chunk, :source)
    document_id = Map.get(chunk, "document_id") || Map.get(chunk, :document_id)
    page = Map.get(chunk, "page") || Map.get(chunk, :page) || 1

    if is_binary(source) and not is_nil(document_id),
      do: {{document_id, page}, document_id, source, page},
      else: nil
  end

  defp sources_frame(acc, sources) do
    acc
    |> chunk(%{}, nil)
    |> Map.put(:zaq_sources, sources_payload(sources))
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

  defp completion(acc, answer, sources, finish_reason) do
    %{
      id: acc.id,
      object: "chat.completion",
      created: acc.created,
      model: acc.model,
      choices: [
        %{
          index: 0,
          message: %{role: "assistant", content: answer},
          finish_reason: finish_reason
        }
      ],
      zaq_sources: sources_payload(sources)
    }
  end

  defp stream_error(acc, reason) do
    %{
      id: acc.id,
      object: "chat.completion.chunk",
      created: acc.created,
      model: acc.model,
      error: %{message: error_message(reason), type: "server_error"}
    }
  end

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
  # the run question (see `run/5`), never persisted.
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
    case chunk_out(conn, "data: #{Jason.encode!(event)}\n\n") do
      {:ok, conn} -> conn
      {:error, _reason} -> conn
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
  defp error_message(:timeout), do: "La réponse a pris trop de temps. Veuillez réessayer."
  defp error_message(reason) when is_binary(reason), do: reason
  defp error_message(_reason), do: "Une erreur est survenue. Veuillez réessayer."

  defp metadata_get(map, key) when is_map(map) and is_atom(key),
    do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  defp metadata_get(_map, _key), do: nil

  defp node_router_module do
    Application.get_env(:zaq, :chat_completions_node_router_module, Zaq.NodeRouter)
  end

  defp fetch(map, key) when is_map(map), do: Map.get(map, key)
  defp fetch(_map, _key), do: nil
end
