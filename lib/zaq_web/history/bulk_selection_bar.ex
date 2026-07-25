defmodule ZaqWeb.History.BulkSelectionBar do
  @moduledoc """
  Bulk actions bar when one or more conversations are selected on the history page.
  """

  use Phoenix.Component

  import ZaqWeb.Components.DesignSystem.Table, only: [table_selection_bar: 1]

  alias ZaqWeb.Components.DesignSystem.Button, as: DSButton

  attr :selected_count, :integer, required: true, doc: "When zero, render nothing."
  attr :live_action, :atom, required: true, doc: "Hide Archive bulk action on archived route."

  def bulk_selection_bar(assigns) do
    ~H"""
    <.table_selection_bar selected_count={@selected_count} class="mb-4">
      <:actions>
        <DSButton.button
          :if={@live_action != :archived}
          variant={:tertiary}
          phx-click="bulk_archive"
        >
          Archive
        </DSButton.button>
        <DSButton.button
          variant={:tertiary}
          danger
          phx-click="bulk_delete"
          data-confirm={"Delete #{@selected_count} conversation(s)? This cannot be undone."}
        >
          Delete
        </DSButton.button>
      </:actions>
    </.table_selection_bar>
    """
  end
end
