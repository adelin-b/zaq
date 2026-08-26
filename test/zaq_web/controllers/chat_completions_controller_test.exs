defmodule ZaqWeb.ChatCompletionsControllerTest do
  # async: false — tests mutate the ZAQ_CHAT_TOKEN process-global env var.
  use ZaqWeb.ConnCase, async: false

  import Ecto.Query, only: [from: 2]

  alias Zaq.Accounts.People
  alias Zaq.Engine.Conversations

  @path "/v1/chat/completions"
  @token "test-chat-token"

  setup %{conn: conn} do
    previous = System.get_env("ZAQ_CHAT_TOKEN")
    System.put_env("ZAQ_CHAT_TOKEN", @token)

    on_exit(fn ->
      if previous,
        do: System.put_env("ZAQ_CHAT_TOKEN", previous),
        else: System.delete_env("ZAQ_CHAT_TOKEN")
    end)

    {:ok, conn: put_req_header(conn, "content-type", "application/json")}
  end

  defp authed(conn), do: put_req_header(conn, "authorization", "Bearer #{@token}")

  defp body(overrides \\ %{}) do
    Map.merge(
      %{
        "model" => "zaq-chat",
        "stream" => true,
        "user" => "user-a",
        "conversation_id" => Ecto.UUID.generate(),
        "messages" => [%{"role" => "user", "content" => "Bonjour"}]
      },
      overrides
    )
  end

  # ---------------------------------------------------------------------------
  # Auth — fail-closed bearer gate.
  # ---------------------------------------------------------------------------

  test "503 when ZAQ_CHAT_TOKEN is not configured", %{conn: conn} do
    System.delete_env("ZAQ_CHAT_TOKEN")
    conn = post(authed(conn), @path, body())
    assert json_response(conn, 503)["error"]["message"] =~ "not configured"
  end

  test "401 without bearer", %{conn: conn} do
    conn = post(conn, @path, body())
    assert json_response(conn, 401)["error"]["message"] == "missing bearer token"
  end

  test "403 with wrong bearer", %{conn: conn} do
    conn = conn |> put_req_header("authorization", "Bearer nope") |> post(@path, body())
    assert json_response(conn, 403)["error"]["message"] == "invalid bearer token"
  end

  # ---------------------------------------------------------------------------
  # Request validation.
  # ---------------------------------------------------------------------------

  test "400 when no user message present", %{conn: conn} do
    conn =
      post(authed(conn), @path, body(%{"messages" => [%{"role" => "system", "content" => "x"}]}))

    assert json_response(conn, 400)["error"]["message"] == "no user message provided"
  end

  test "400 when user (owner id) missing", %{conn: conn} do
    conn = post(authed(conn), @path, Map.delete(body(), "user"))
    assert json_response(conn, 400)["error"]["message"] =~ "user"
  end

  test "400 when conversation_id missing", %{conn: conn} do
    conn = post(authed(conn), @path, Map.delete(body(), "conversation_id"))
    assert json_response(conn, 400)["error"]["message"] =~ "conversation_id"
  end

  test "400 when conversation_id is not a UUID", %{conn: conn} do
    conn = post(authed(conn), @path, body(%{"conversation_id" => "not-a-uuid"}))
    assert json_response(conn, 400)["error"]["message"] == "invalid conversation_id"
  end

  test "413 when messages exceed the cap", %{conn: conn} do
    messages = List.duplicate(%{"role" => "user", "content" => "x"}, 201)
    conn = post(authed(conn), @path, body(%{"messages" => messages}))
    assert json_response(conn, 413)["error"]["message"] =~ "too many messages"
  end

  # ---------------------------------------------------------------------------
  # Ownership gate (IDOR guard) — rejected BEFORE any agent run.
  # ---------------------------------------------------------------------------

  test "403 when the conversation belongs to another user", %{conn: conn} do
    convo_id = Ecto.UUID.generate()
    assert {:ok, _} = Conversations.create_chat_conversation(convo_id, "user-a")

    conn = post(authed(conn), @path, body(%{"user" => "user-b", "conversation_id" => convo_id}))
    assert json_response(conn, 403)["error"]["message"] == "conversation does not belong to user"
  end

  test "403 when the conversation exists on another channel type", %{conn: conn} do
    convo_id = Ecto.UUID.generate()
    assert {:ok, _} = Conversations.create_chat_conversation(convo_id, "user-a")
    # Same owner but wrong channel_type must not resolve either.
    {1, _} =
      Zaq.Repo.update_all(
        from(c in Zaq.Engine.Conversations.Conversation, where: c.id == ^convo_id),
        set: [channel_type: "bo"]
      )

    conn = post(authed(conn), @path, body(%{"user" => "user-a", "conversation_id" => convo_id}))
    assert json_response(conn, 403)["error"]["message"] == "conversation does not belong to user"
  end

  # ---------------------------------------------------------------------------
  # Conversation lifecycle helper.
  # ---------------------------------------------------------------------------

  test "create_chat_conversation surfaces a pk race as {:error, changeset}, not a raise" do
    convo_id = Ecto.UUID.generate()
    assert {:ok, conv} = Conversations.create_chat_conversation(convo_id, "user-a")
    assert conv.channel_type == "chat"
    assert conv.channel_user_id == "user-a"

    assert {:error, %Ecto.Changeset{}} =
             Conversations.create_chat_conversation(convo_id, "user-b")
  end

  # ---------------------------------------------------------------------------
  # Pipeline routing — the run flows through route_incoming_message/4 (like
  # every other channel bridge) and the result comes back over ChatBridge
  # PubSub as {:chat_result, request_id, outgoing}.
  # ---------------------------------------------------------------------------

  defmodule EchoRouter do
    @moduledoc false
    alias Zaq.Channels.ChatBridge
    alias Zaq.Engine.IncomingMessageRouter
    alias Zaq.Engine.Messages.{Incoming, Outgoing}
    alias Zaq.Event

    def dispatch(%Event{} = event) do
      event = IncomingMessageRouter.route(event)
      %Event{request: %Incoming{} = incoming} = event
      topic = ChatBridge.topic(incoming.channel_id)

      # Progressive cumulative stream deltas, like StreamEvents flushes them —
      # including a partially-streamed source marker that must be held back.
      for cumulative <- [
            "Réponse ",
            "Réponse générée. [[.sou",
            "Réponse générée. [[.source:doc.pdf|p3]]"
          ] do
        Phoenix.PubSub.broadcast(
          Zaq.PubSub,
          topic,
          {:chat_stream_delta, incoming.message_id, cumulative}
        )
      end

      outgoing = %Outgoing{
        body: "Réponse générée. [[source:doc.pdf|p3]]",
        channel_id: incoming.channel_id,
        provider: :chat,
        in_reply_to: incoming.message_id,
        metadata:
          Map.merge(incoming.metadata, %{
            tool_calls: [
              %{
                "name" => "retrieve",
                "response" => %{
                  "chunks" => [
                    %{"source" => "doc.pdf", "document_id" => "doc-1", "page" => 3}
                  ]
                }
              }
            ]
          })
      }

      Phoenix.PubSub.broadcast(Zaq.PubSub, topic, {:chat_result, incoming.message_id, outgoing})

      %{event | response: nil}
    end
  end

  # EchoRouter's cumulative sequence is strictly monotonic, so it never exercises
  # a ReAct segment reset. This one restarts the accumulator mid-run — the normal
  # shape of a multi-step agentic answer.
  defmodule SegmentResetRouter do
    @moduledoc false
    alias Zaq.Channels.ChatBridge
    alias Zaq.Engine.IncomingMessageRouter
    alias Zaq.Engine.Messages.{Incoming, Outgoing}
    alias Zaq.Event

    @final "Le conseil a voté le budget."

    def dispatch(%Event{} = event) do
      event = IncomingMessageRouter.route(event)
      %Event{request: %Incoming{} = incoming} = event
      topic = ChatBridge.topic(incoming.channel_id)

      for cumulative <- [
            "Je vais",
            "Je vais chercher dans les documents.",
            # New segment: no longer extends what was sent.
            "Le",
            "Le conseil a voté",
            @final
          ] do
        Phoenix.PubSub.broadcast(
          Zaq.PubSub,
          topic,
          {:chat_stream_delta, incoming.message_id, cumulative}
        )
      end

      outgoing = %Outgoing{
        body: @final,
        channel_id: incoming.channel_id,
        provider: :chat,
        in_reply_to: incoming.message_id,
        metadata: incoming.metadata
      }

      Phoenix.PubSub.broadcast(Zaq.PubSub, topic, {:chat_result, incoming.message_id, outgoing})

      %{event | response: nil}
    end
  end

  defmodule SilentRouter do
    @moduledoc false
    def dispatch(%Zaq.Event{} = event), do: %{event | response: nil}
  end

  defp with_router(router) do
    Application.put_env(:zaq, :chat_completions_node_router_module, router)
    on_exit(fn -> Application.delete_env(:zaq, :chat_completions_node_router_module) end)
  end

  test "non-stream: pipeline result folds into an OpenAI completion with citations",
       %{conn: conn} do
    with_router(EchoRouter)

    conn = post(authed(conn), @path, body(%{"stream" => false}))
    resp = json_response(conn, 200)

    assert %{"choices" => [%{"message" => %{"role" => "assistant", "content" => content}}]} =
             resp

    # Inline source markers are stripped — citations ride zaq_sources instead.
    assert content == "Réponse générée."

    assert [%{"sourceId" => "doc-1", "page" => 3, "url" => "/chat/documents/doc-1?page=3"}] =
             Enum.map(resp["zaq_sources"], &Map.take(&1, ["sourceId", "page", "url"]))
  end

  test "stream: progressive deltas + zaq_sources + [DONE]", %{conn: conn} do
    with_router(EchoRouter)

    conn = post(authed(conn), @path, body())

    assert conn.status == 200
    sse = response(conn, 200)
    assert sse =~ ~s("delta":{"role":"assistant"})

    # Progressive: the answer arrives across MULTIPLE content deltas that
    # concatenate to the clean answer — markers never leak, the held-back
    # partial marker ("[[sou") is dropped once it completes.
    deltas =
      sse
      |> String.split("\n\n", trim: true)
      |> Enum.map(&String.trim_leading(&1, "data: "))
      |> Enum.reject(&(&1 == "[DONE]"))
      |> Enum.map(&Jason.decode!/1)
      |> Enum.flat_map(fn frame ->
        case frame do
          %{"choices" => [%{"delta" => %{"content" => content}}]} -> [content]
          _ -> []
        end
      end)

    assert length(deltas) > 1
    assert Enum.join(deltas) == "Réponse générée."
    refute sse =~ "[[source"
    refute sse =~ "[[.source"

    sources_at = :binary.match(sse, "zaq_sources") |> elem(0)
    stop_at = :binary.match(sse, ~s("finish_reason":"stop")) |> elem(0)
    assert sources_at < stop_at
    assert sse =~ "data: [DONE]"
  end

  # ---------------------------------------------------------------------------
  # People directory — the run resolves a Person from the `chat` channel
  # identity, named and matched from the caller-supplied `zaq_user`.
  # ---------------------------------------------------------------------------

  # Each call is its own request (and its own conversation), so a multi-turn
  # test exercises the same path a real caller's successive turns take.
  defp ask_as(user_id, overrides) do
    build_conn()
    |> put_req_header("content-type", "application/json")
    |> authed()
    |> post(@path, body(Map.merge(%{"stream" => false, "user" => user_id}, overrides)))
  end

  test "creates one People entry per caller, named from zaq_user" do
    with_router(EchoRouter)
    user_id = "supabase-" <> Ecto.UUID.generate()

    assert json_response(ask_as(user_id, %{"zaq_user" => %{"name" => "Adelin Bérard"}}), 200)

    assert {:ok, person} = People.match_by_channel("chat", user_id)
    assert person.full_name == "Adelin Bérard"
  end

  @tag :security
  test "a caller-supplied email cannot hijack an existing Person" do
    with_router(EchoRouter)

    # A Person that exists independently of the chat channel, with teams.
    {:ok, victim} =
      People.create_person(%{
        full_name: "Marie Dupont",
        email: "marie@client.com",
        team_ids: [1234]
      })

    user_id = "supabase-" <> Ecto.UUID.generate()

    assert json_response(
             ask_as(user_id, %{
               "zaq_user" => %{"name" => "Attacker", "email" => "marie@client.com"}
             }),
             200
           )

    # The chat identity keys a NEW, team-less Person: the email is ignored, so
    # match_by_email can never select Marie's entry.
    assert {:ok, chat_person} = People.match_by_channel("chat", user_id)
    refute chat_person.id == victim.id
    assert chat_person.team_ids in [nil, []]

    # Marie's directory entry is untouched — name and teams both intact.
    reloaded = Zaq.Repo.get!(Zaq.Accounts.Person, victim.id)
    assert reloaded.full_name == "Marie Dupont"
    assert reloaded.team_ids == [1234]
  end

  test "reuses the same Person across turns instead of creating one per request" do
    with_router(EchoRouter)
    user_id = "supabase-" <> Ecto.UUID.generate()
    identity = %{"zaq_user" => %{"name" => "Jeanne Martin"}}

    assert json_response(ask_as(user_id, identity), 200)
    assert {:ok, first} = People.match_by_channel("chat", user_id)

    assert json_response(ask_as(user_id, identity), 200)
    assert {:ok, second} = People.match_by_channel("chat", user_id)

    assert first.id == second.id
    assert [%{platform: "chat", channel_identifier: ^user_id}] = second.channels
  end

  test "renames a Person that was seeded from the raw user id once a name arrives" do
    with_router(EchoRouter)
    user_id = "supabase-" <> Ecto.UUID.generate()

    # First turn without zaq_user: nothing to name the person with.
    assert json_response(ask_as(user_id, %{}), 200)
    assert {:ok, seeded} = People.match_by_channel("chat", user_id)
    assert seeded.full_name == user_id

    assert json_response(ask_as(user_id, %{"zaq_user" => %{"name" => "Paul Durand"}}), 200)

    assert {:ok, healed} = People.match_by_channel("chat", user_id)
    assert healed.id == seeded.id
    assert healed.full_name == "Paul Durand"
  end

  defp sse_content_deltas(sse) do
    sse
    |> String.split("\n\n", trim: true)
    |> Enum.map(&String.trim_leading(&1, "data: "))
    |> Enum.reject(&(&1 == "[DONE]"))
    |> Enum.map(&Jason.decode!/1)
    |> Enum.flat_map(fn
      %{"choices" => [%{"delta" => %{"content" => content}} | _]} -> [content]
      _ -> []
    end)
  end

  test "keeps streaming after a ReAct segment restarts the accumulator", %{conn: conn} do
    with_router(SegmentResetRouter)

    sse = conn |> authed() |> post(@path, body()) |> response(200)
    deltas = sse_content_deltas(sse)

    # The second segment must arrive as MULTIPLE progressive deltas, not as one
    # blocking chunk appended by finish_answer — that is the whole feature.
    second_segment = deltas |> Enum.drop_while(&(not String.contains?(&1, "Le"))) |> Enum.count()
    assert second_segment > 1

    joined = Enum.join(deltas)
    assert String.contains?(joined, "Le conseil a voté le budget.")
    # The final answer is delivered exactly once — not duplicated by both the
    # stream and finish_answer's reconciliation.
    assert joined |> String.split("Le conseil a voté le budget.") |> length() == 2
  end

  test "streaming errors carry a choices frame an OpenAI client can render", %{conn: conn} do
    with_router(SilentRouter)
    Application.put_env(:zaq, :chat_result_timeout_ms, 50)
    on_exit(fn -> Application.delete_env(:zaq, :chat_result_timeout_ms) end)

    # stream: true — the fixture default. The status is already committed to 200
    # by the time the timeout fires, so the error has to travel in-band.
    sse = conn |> authed() |> post(@path, body()) |> response(200)

    frames =
      sse
      |> String.split("\n\n", trim: true)
      |> Enum.map(&String.trim_leading(&1, "data: "))
      |> Enum.reject(&(&1 == "[DONE]"))
      |> Enum.map(&Jason.decode!/1)

    error_frame = Enum.find(frames, &Map.has_key?(&1, "error"))
    assert error_frame["error"]["message"] =~ "trop de temps"

    # A client that only reads `choices` must still surface the message.
    assert [%{"delta" => %{"content" => content}, "finish_reason" => "stop"}] =
             error_frame["choices"]

    assert content =~ "trop de temps"
    assert sse =~ "data: [DONE]"
  end

  test "citations are emitted before the terminal stop chunk", %{conn: conn} do
    with_router(EchoRouter)

    sse = conn |> authed() |> post(@path, body()) |> response(200)

    sources_at = :binary.match(sse, "zaq_sources") |> elem(0)
    stop_at = :binary.match(sse, ~s("finish_reason":"stop")) |> elem(0)

    # A spec-compliant client stops consuming at finish_reason, so sources after
    # it are silently dropped.
    assert sources_at < stop_at
  end

  test "502 when no pipeline result arrives before the timeout", %{conn: conn} do
    with_router(SilentRouter)
    Application.put_env(:zaq, :chat_result_timeout_ms, 50)
    on_exit(fn -> Application.delete_env(:zaq, :chat_result_timeout_ms) end)

    conn = post(authed(conn), @path, body(%{"stream" => false}))
    assert json_response(conn, 502)["error"]["message"] =~ "trop de temps"
  end
end
