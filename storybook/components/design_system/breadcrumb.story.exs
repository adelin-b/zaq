defmodule Storybook.Components.DesignSystem.Breadcrumb do
  use PhoenixStorybook.Story, :page
  use Phoenix.Component

  import ZaqWeb.Components.DesignSystem.Breadcrumb

  def description, do: "BO breadcrumbs — ingestion file browser and page hierarchy."

  def render(assigns) do
    ~H"""
    <div style="padding: var(--zaq-scale-32); display: flex; flex-direction: column; gap: var(--zaq-scale-24);">
      <section>
        <p
          class="zaq-text-body-sm"
          style="margin-bottom: var(--zaq-scale-8); color: var(--zaq-text-color-body-secondary);"
        >
          Page hierarchy
        </p>
        <.page_breadcrumb items={[
          %{label: "Workflows", to: "/bo/workflows"},
          %{label: "Lead intake", to: "/bo/workflows/demo"},
          %{label: "Run abc12345", current: true}
        ]} />
      </section>
      <section>
        <p
          class="zaq-text-body-sm"
          style="margin-bottom: var(--zaq-scale-8); color: var(--zaq-text-color-body-secondary);"
        >
          Ingestion file browser
        </p>
        <.breadcrumb
          breadcrumbs={[
            %{name: "docs", path: "docs"},
            %{name: "sub", path: "docs/sub"}
          ]}
          current_dir="docs/sub"
        />
      </section>
    </div>
    """
  end
end
