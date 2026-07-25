defmodule ZaqWeb.History.ConversationRow do
  @moduledoc """
  One data row on the BO conversation history table.
  """

  use ZaqWeb, :html

  import ZaqWeb.Components.DesignSystem.Table,
    only: [
      table_actions: 1,
      table_badge: 1,
      table_cell: 1,
      table_checkbox: 1,
      table_datetime: 1,
      table_row: 1,
      table_text: 1
    ]

  alias ZaqWeb.Components.DesignSystem.Button, as: DSButton

  attr :conversation, :any,
    required: true,
    doc: "Preloaded conversation struct (engine list result)."

  attr :selected, :any, required: true, doc: "MapSet of selected conversation ids."
  attr :live_action, :atom, required: true
  attr :show_identity?, :boolean, required: true

  def conversation_row(assigns) do
    ~H"""
    <.table_row
      id={"conv-#{@conversation.id}"}
      navigate={~p"/bo/conversations/#{@conversation.id}"}
      variant={if(MapSet.member?(@selected, @conversation.id), do: :selected, else: :default)}
    >
      <.table_cell width="w-10">
        <.table_checkbox
          checked={MapSet.member?(@selected, @conversation.id)}
          phx-click="toggle_select"
          phx-value-id={@conversation.id}
        />
      </.table_cell>

      <.table_cell class="max-w-xs">
        <.table_text label={@conversation.title || "(untitled)"} tone={:mono} truncate />
      </.table_cell>

      <.table_cell :if={@show_identity?}>
        <.link
          :if={@conversation.person}
          navigate={~p"/bo/people?person_id=#{@conversation.person.id}"}
          class="font-mono text-[0.7rem] text-[#03b6d4] hover:underline"
          onclick="event.stopPropagation()"
        >
          {@conversation.person.full_name}
        </.link>
        <.table_text
          :if={is_nil(@conversation.person) && @conversation.user}
          label={@conversation.user.username}
          tone={:secondary}
          class="font-mono text-[0.7rem]"
        />
        <.table_text
          :if={
            is_nil(@conversation.person) && is_nil(@conversation.user) &&
              @conversation.channel_user_id
          }
          label={@conversation.channel_user_id}
          tone={:secondary}
          class="font-mono text-[0.7rem]"
        />
        <.table_text
          :if={
            is_nil(@conversation.person) && is_nil(@conversation.user) &&
              is_nil(@conversation.channel_user_id)
          }
          label="—"
          tone={:tertiary}
          class="font-mono text-[0.7rem]"
        />
      </.table_cell>

      <.table_cell>
        <.table_badge status="processing">
          {@conversation.channel_type}
        </.table_badge>
      </.table_cell>

      <.table_cell>
        <.table_datetime value={@conversation.inserted_at} />
      </.table_cell>

      <.table_cell>
        <.table_datetime value={@conversation.updated_at} />
      </.table_cell>

      <.table_cell align={:right} nowrap>
        <.table_actions>
          <DSButton.button
            :if={@live_action != :archived}
            variant={:tertiary}
            phx-click="archive_conversation"
            phx-value-id={@conversation.id}
          >
            Archive
          </DSButton.button>
          <DSButton.button
            variant={:tertiary}
            danger
            phx-click="delete_conversation"
            phx-value-id={@conversation.id}
            data-confirm="Delete this conversation? This cannot be undone."
          >
            Delete
          </DSButton.button>
        </.table_actions>
      </.table_cell>
    </.table_row>
    """
  end
end
