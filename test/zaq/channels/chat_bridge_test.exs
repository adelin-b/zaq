defmodule Zaq.Channels.ChatBridgeTest do
  use ExUnit.Case, async: true

  alias Zaq.Channels.{Api, ChatBridge}
  alias Zaq.Event

  defmodule MissingConfigBridge do
    def bridge_for(:chat), do: ChatBridge
    def fetch_channel_config(:chat), do: {:error, {:channel_not_configured, :chat}}
    def fetch_connection_details(:chat), do: %{}
  end

  test "acknowledges typing without channel config or an external transport" do
    event =
      Event.new(%{provider: :chat, channel_id: "conversation-id"}, :channels,
        opts: [action: :send_typing, bridge_module: MissingConfigBridge]
      )

    assert %Event{response: :ok} = Api.handle_event(event, :send_typing, nil)
  end
end
