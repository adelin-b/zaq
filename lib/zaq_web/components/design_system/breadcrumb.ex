defmodule ZaqWeb.Components.DesignSystem.Breadcrumb do
  @moduledoc """
  BO breadcrumb components — ingestion file browser and page hierarchy navigation.

  - `breadcrumb/1` — in-page folder trail with back control (ingestion file browser).
  - `page_breadcrumb/1` — route-based hierarchy via LiveView `navigate` links.

  **Styles:** universal block in `assets/css/styles.css` — `.zaq-breadcrumb-*`,
  plus shared `.zaq-icon-sm` and `.zaq-link-underline` (not under the ingestion-only section).
  """

  use Phoenix.Component

  alias ZaqWeb.Components.DesignSystem.Link, as: DSLink

  attr :breadcrumbs, :list, required: true
  attr :current_dir, :string, required: true

  def breadcrumb(assigns) do
    assigns = assign(assigns, :visible?, breadcrumb_visible?(assigns))

    ~H"""
    <div :if={@visible?} class="zaq-breadcrumb-row zaq-text-body-sm">
      <button
        :if={@current_dir != "."}
        phx-click="go_back"
        class="zaq-breadcrumb-back-btn"
        title="Go back"
        type="button"
      >
        <svg
          class="zaq-icon-sm"
          fill="none"
          stroke="currentColor"
          stroke-width="2.5"
          viewBox="0 0 24 24"
        >
          <path stroke-linecap="round" stroke-linejoin="round" d="M15 19l-7-7 7-7" />
        </svg>
      </button>
      <button
        phx-click="navigate"
        phx-value-path="."
        class="zaq-link-underline zaq-breadcrumb-crumb-link"
        type="button"
      >
        root
      </button>
      <span :for={crumb <- @breadcrumbs} class="zaq-breadcrumb-trail">
        <span class="zaq-breadcrumb-sep">/</span>
        <button
          phx-click="navigate"
          phx-value-path={crumb.path}
          class="zaq-link-underline zaq-breadcrumb-crumb-link"
          type="button"
        >
          {crumb.name}
        </button>
      </span>
    </div>
    """
  end

  attr :items, :list,
    required: true,
    doc:
      "Trail items — `%{label:, to:}` for links, `%{label:, current: true}` for the terminal crumb."

  attr :id, :string, default: nil

  def page_breadcrumb(assigns) do
    ~H"""
    <nav
      :if={@items != []}
      id={@id}
      class="zaq-breadcrumb-row zaq-text-body-sm"
      aria-label="Breadcrumb"
    >
      <%= for {item, index} <- Enum.with_index(@items) do %>
        <.page_breadcrumb_item :if={index == 0} item={item} />
        <span :if={index > 0} class="zaq-breadcrumb-trail">
          <span class="zaq-breadcrumb-sep">/</span>
          <.page_breadcrumb_item item={item} />
        </span>
      <% end %>
    </nav>
    """
  end

  attr :item, :map, required: true

  defp page_breadcrumb_item(%{item: %{current: true}} = assigns) do
    ~H"""
    <span class="zaq-breadcrumb-current">{@item.label}</span>
    """
  end

  defp page_breadcrumb_item(%{item: %{to: to}} = assigns) when is_binary(to) do
    ~H"""
    <DSLink.nav_link destination={@item.to} size={:sm} tone={:accent}>
      {@item.label}
    </DSLink.nav_link>
    """
  end

  defp page_breadcrumb_item(assigns) do
    ~H"""
    <span class="zaq-breadcrumb-current">{@item.label}</span>
    """
  end

  defp breadcrumb_visible?(%{current_dir: ".", breadcrumbs: []}), do: false
  defp breadcrumb_visible?(_), do: true
end
