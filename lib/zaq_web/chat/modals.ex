defmodule ZaqWeb.Chat.Modals do
  @moduledoc """
  BO Chat — delete confirmation and negative-feedback dialogs.
  """

  use Phoenix.Component

  import ZaqWeb.Components.BOModal, only: [form_dialog: 1]

  alias ZaqWeb.Components.DesignSystem.Button, as: DSButton

  attr :show_delete_confirm, :boolean, required: true

  def delete_confirm_modal(assigns) do
    ~H"""
    <%= if @show_delete_confirm do %>
      <div
        id="delete-confirm-modal"
        class="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm"
        phx-click="close_delete_modal"
      >
        <div
          class="bg-white rounded-2xl shadow-2xl w-full max-w-sm mx-4 overflow-hidden"
          phx-click="noop"
        >
          <div class="px-6 pt-6 pb-4">
            <h3 class="text-base font-semibold mb-2" style="color:#2c2b28;">Delete this chat?</h3>
            <p class="text-sm" style="color:#9e9b94;">
              This conversation will be permanently deleted and cannot be recovered.
            </p>
          </div>
          <div class="flex items-center justify-end gap-3 px-6 pb-5">
            <button
              id="delete-modal-cancel"
              phx-click="close_delete_modal"
              class="px-4 py-2 text-sm transition-colors"
              style="color:#9e9b94;"
            >
              Cancel
            </button>
            <button
              id="delete-modal-confirm"
              phx-click="delete_chat"
              class="px-5 py-2 text-sm font-medium text-white rounded-lg transition-all active:scale-95"
              style="background:#ef4444;"
            >
              Delete
            </button>
          </div>
        </div>
      </div>
    <% end %>
    """
  end

  attr :show_feedback_modal, :boolean, required: true
  attr :feedback_reasons, :list, required: true
  attr :feedback_comment, :string, required: true

  def feedback_modal(assigns) do
    ~H"""
    <.form_dialog
      :if={@show_feedback_modal}
      id="feedback-modal"
      cancel_event="close_feedback_modal"
      title="Provide feedback"
      max_width_class="zaq-modal--width-md"
    >
      <form id="feedback-modal-form" phx-submit="submit_feedback" class="zaq-layout-stack">
        <div class="flex flex-wrap gap-2">
          <DSButton.button
            :for={reason <- Zaq.Engine.Telemetry.FeedbackReasons.list()}
            type="button"
            variant={:secondary}
            shape={:pill}
            active={reason in @feedback_reasons}
            phx-click="toggle_feedback_reason"
            phx-value-reason={reason}
            data-reason-selected={to_string(reason in @feedback_reasons)}
          >
            {reason}
          </DSButton.button>
        </div>
        <textarea
          id="feedback-modal-comment"
          phx-change="update_feedback_comment"
          name="comment"
          rows="4"
          placeholder="Tell us more (optional)"
          class="w-full zaq-control-text resize-none"
        ><%= @feedback_comment %></textarea>
      </form>
      <:actions>
        <div class="zaq-layout-inline">
          <DSButton.button
            type="button"
            variant={:secondary}
            phx-click="close_feedback_modal"
          >
            Cancel
          </DSButton.button>
          <DSButton.button
            id="submit-feedback-button"
            type="submit"
            variant={:primary}
            form="feedback-modal-form"
          >
            Submit
          </DSButton.button>
        </div>
      </:actions>
    </.form_dialog>
    """
  end
end
