defmodule ZaqWeb.ChatCompletionsControllerTest do
  # async: false — tests mutate the ZAQ_CHAT_TOKEN process-global env var.
  use ZaqWeb.ConnCase, async: false

  import Ecto.Query, only: [from: 2]

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
end
