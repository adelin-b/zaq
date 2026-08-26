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

  Requests flow through `CommunicationBridge.route_incoming_message/4` →
  `NodeRouter.dispatch/1` like every other channel bridge, so traces, Person
  resolution (`Zaq.People.IdentityResolver`) and conversation persistence come
  from the shared pipeline. The transport is a synchronous HTTP request, so the
  controller subscribes to `Zaq.Channels.ChatBridge.topic/1` BEFORE routing and
  blocks until `ChatBridge.send_reply/2` broadcasts the pipeline `%Outgoing{}`
  back as `{:chat_result, request_id, outgoing}`.

  Progressive streaming is preserved: the executor's `StreamEvents` flushes the
  in-progress answer as `:stream_delta` upserts, `ChatBridge.upsert_message/3`
  forwards them here as `{:chat_stream_delta, request_id, cumulative}`, and the
  controller emits the not-yet-sent suffix as OpenAI content deltas while the
  LLM is still generating. The final `{:chat_result, …}` reconciles the
  authoritative answer with what was already streamed.

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
  - The bearer token authenticates the calling *service*, never the end user, so
    every identity field in the body (`user`, `zaq_user.name`) is an unverified
    claim. They may therefore only ever key a Person the chat channel itself
    owns. `zaq_user` deliberately carries NO email: `People.match_person/1`
    matches on email first and platform-agnostically, so accepting one would let
    any token holder select an arbitrary existing Person by guessing its
    address, inherit its `team_ids` (widening retrieval past public documents)
    and overwrite its `full_name`. See `Zaq.People.Resolver.normalize/2`.
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
    topic = ChatBridge.topic(convo_id)
    # Subscribe before routing so the reply broadcast can't win the race, and
    # ALWAYS release it: Bandit serves successive keep-alive requests in the
    # same process, so a leaked subscription would keep delivering another
    # conversation's completions into this process's mailbox forever.
    :ok = Phoenix.PubSub.subscribe(Zaq.PubSub, topic)

    try do
      do_run(conn, params, question, convo_id, user_id)
    after
      Phoenix.PubSub.unsubscribe(Zaq.PubSub, topic)
    end
  end

  defp do_run(conn, params, question, convo_id, user_id) do
    request_id = Ecto.UUID.generate()

    incoming =
      ChatBridge.to_internal(%{
        content: question,
        conversation_id: convo_id,
        author_id: user_id,
        author_name: zaq_user_field(params, "name"),
        message_id: request_id,
        source_filter: parse_source_filter(params)
      })

    acc = %{
      conn: conn,
      id: "chatcmpl-" <> Integer.to_string(System.unique_integer([:positive])),
      created: created_ts(),
      model: fetch(params, "model") || "zaq-chat",
      stream?: stream?(params),
      # Progressive streaming state: SSE headers/role frame are sent lazily on
      # the first delta; `sent` tracks the bytes already on the wire so the
      # final answer only appends its remainder.
      sse_started?: false,
      role_sent?: false,
      # Bytes already on the wire for the CURRENT ReAct segment; reset when a
      # new segment restarts the accumulator (see push_stream/2).
      sent: ""
    }

    # Grounding (an optional OpenAI `system` message) frames THIS run only —
    # passed as the run question so it is injected into retrieval but never
    # persisted (the stored user turn stays the clean question).
    case route(incoming, with_system(system_content(params), question)) do
      # Sync hop: the pipeline result came straight back.
      %Outgoing{} = outgoing -> respond(acc, outgoing)
      # Async hop: deltas + result arrive via ChatBridge broadcasts over PubSub.
      :ok -> await_result(acc, request_id)
      {:error, reason} -> respond_error(acc, reason)
    end
  end

  defp route(incoming, run_question) do
    CommunicationBridge.route_incoming_message(
      incoming,
      # No BO channel-config surface for chat: without a global default agent,
      # pin the default answering executor (agentic run + tool citations)
      # instead of falling back to the legacy pipeline.
      [question: run_question, default_answering_executor: true],
      actor_from_incoming(incoming),
      node_router: node_router_module()
    )
  end

  defp actor_from_incoming(incoming) do
    %{id: incoming.author_id, name: incoming.author_name, provider: incoming.provider}
  end

  # Idle timeout: the clock restarts on every delta, so a generating run is
  # never cut off mid-answer — only a silent pipeline trips it.
  defp await_result(acc, request_id) do
    timeout = Application.get_env(:zaq, :chat_result_timeout_ms, @default_result_timeout_ms)

    receive do
      {:chat_stream_delta, ^request_id, cumulative} ->
        acc |> push_stream(cumulative) |> await_result(request_id)

      {:chat_result, ^request_id, %Outgoing{} = outgoing} ->
        respond(acc, outgoing)
    after
      timeout -> respond_error(acc, :timeout)
    end
  end

  # ---------------------------------------------------------------------------
  # Progressive deltas — `cumulative` is the answer text so far for the current
  # LLM-call segment. Emit only the suffix not yet on the wire; use the same
  # cleanup as the terminal result, then hold back any partial source marker.
  # If the text stops extending what was sent (a later ReAct
  # segment restarted the accumulator), stop streaming and let the final
  # result reconcile.
  # ---------------------------------------------------------------------------

  defp push_stream(%{stream?: false} = acc, _cumulative), do: acc

  defp push_stream(acc, cumulative) when is_binary(cumulative) do
    emittable =
      cumulative |> AnsweringRun.clean_answer() |> safe_stream_prefix() |> ltrim_if_new(acc)

    cond do
      emittable == acc.sent ->
        acc

      String.starts_with?(emittable, acc.sent) ->
        delta =
          binary_part(emittable, byte_size(acc.sent), byte_size(emittable) - byte_size(acc.sent))

        acc
        |> ensure_sse_role()
        |> emit_acc(&chunk(&1, %{content: delta}, nil))
        |> Map.put(:sent, emittable)

      true ->
        restart_segment(acc, emittable)
    end
  end

  defp push_stream(acc, _cumulative), do: acc

  # The cumulative text stopped extending what we sent, which means a new ReAct
  # segment restarted the accumulator. Since the controller pins the answering
  # executor, multi-segment runs are the NORMAL case — giving up here would stop
  # streaming for the rest of the run and deliver the real answer as one blocking
  # chunk. Emit a break and keep going, tracking only the new segment.
  defp restart_segment(acc, emittable) do
    case String.trim_leading(emittable) do
      "" ->
        %{acc | sent: ""}

      segment ->
        acc
        |> ensure_sse_role()
        |> emit_acc(&chunk(&1, %{content: "\n\n" <> segment}, nil))
        |> Map.put(:sent, segment)
    end
  end

  defp ltrim_if_new(text, %{sent: ""}), do: String.trim_leading(text)
  defp ltrim_if_new(text, _acc), do: text

  # Hold back a trailing "[", "[[" or unterminated "[[source:…" so a marker
  # split across flushes never leaks onto the wire, and trailing whitespace so
  # the marker regex's leading `\s*` can never retro-eat bytes already sent.
  defp safe_stream_prefix(text) do
    text
    |> cut_partial_marker()
    |> String.trim_trailing("[")
    |> String.trim_trailing()
  end

  defp cut_partial_marker(text) do
    case :binary.matches(text, "[[") do
      [] ->
        text

      matches ->
        {start, _len} = List.last(matches)
        tail = binary_part(text, start, byte_size(text) - start)

        if String.contains?(tail, "]]") do
          text
        else
          binary_part(text, 0, start)
        end
    end
  end

  defp ensure_sse_role(acc) do
    acc =
      if acc.sse_started?, do: acc, else: %{acc | conn: start_sse(acc.conn), sse_started?: true}

    if acc.role_sent? do
      acc
    else
      emit_acc(%{acc | role_sent?: true}, &chunk(&1, %{role: "assistant"}, nil))
    end
  end

  defp emit_acc(acc, frame_fun), do: %{acc | conn: emit(acc.conn, frame_fun.(acc))}

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
    acc = acc |> ensure_sse_role() |> finish_answer(answer)

    # Sources BEFORE the terminal stop chunk: a spec-compliant client stops
    # consuming at `finish_reason`, so anything after it is dropped — and
    # citations are the whole point of the zaq_sources extension.
    conn = if sources == [], do: acc.conn, else: emit(acc.conn, sources_frame(acc, sources))
    conn |> emit(chunk(acc, %{}, "stop")) |> sse_done()
  end

  defp deliver(%{stream?: false} = acc, answer, sources) do
    json(acc.conn, completion(acc, answer, sources, "stop"))
  end

  # Reconcile the authoritative final answer with what streaming already sent.
  defp finish_answer(%{sent: ""} = acc, answer),
    do: emit_acc(acc, &chunk(&1, %{content: answer}, nil))

  defp finish_answer(%{sent: sent} = acc, answer) do
    base = if String.starts_with?(answer, sent), do: sent, else: String.trim_trailing(sent)

    if String.starts_with?(answer, base) do
      case binary_part(answer, byte_size(base), byte_size(answer) - byte_size(base)) do
        "" -> acc
        rest -> emit_acc(acc, &chunk(&1, %{content: rest}, nil))
      end
    else
      # The streamed segment diverged from the final answer (a late ReAct
      # restart): append the authoritative answer after a break rather than
      # leaving the client with a partial intermediate.
      emit_acc(acc, &chunk(&1, %{content: "\n\n" <> answer}, nil))
    end
  end

  defp respond_error(%{stream?: true} = acc, reason) do
    acc =
      if acc.sse_started?,
        do: acc,
        else: %{acc | conn: start_sse(acc.conn), sse_started?: true}

    acc.conn
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

  # The SSE headers are already on the wire by the time most errors surface, so
  # the status is committed at 200 and the error has to travel in-band. A frame
  # carrying ONLY `error` is skipped by any client that pattern-matches on
  # `choices` — the user gets an empty bubble and nothing is surfaced anywhere.
  # Carry the message as content too, so a stock OpenAI client renders it.
  defp stream_error(acc, reason) do
    message = error_message(reason)

    %{
      id: acc.id,
      object: "chat.completion.chunk",
      created: acc.created,
      model: acc.model,
      choices: [%{index: 0, delta: %{content: message}, finish_reason: "stop"}],
      error: %{message: message, type: "server_error"}
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

  # Optional `zaq_user` (a ZAQ extension): `%{"name" => ...}` for the caller
  # identified by the standard `user` id. Chat Completions has no field for a
  # display name and this channel exposes no profile API for ZAQ to fetch one
  # from, so the caller supplies it here — it is what lets `Zaq.People` name the
  # Person instead of filing it under the raw user id. Any other key (notably
  # `email`) is ignored on purpose; see the Security section of the moduledoc.
  defp zaq_user_field(params, key) do
    case fetch(params, "zaq_user") do
      %{} = zaq_user ->
        case fetch(zaq_user, key) do
          value when is_binary(value) -> value
          _ -> nil
        end

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
