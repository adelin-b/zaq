defmodule ZaqWeb.History.ConversationFilters do
  @moduledoc """
  Toolbar for the BO conversation history page: count, admin scope, team/person
  filters, and channel type.
  """

  use Phoenix.Component

  import ZaqWeb.Components.SearchableSelect
  import ZaqWeb.Select, only: [select: 1]

  alias ZaqWeb.Components.DesignSystem.Toggle, as: DSToggle

  @channel_type_options [
    {"All", "all"},
    {"BO", "bo"},
    {"Mattermost", "mattermost"},
    {"Slack", "slack"},
    {"Email", "email:imap"},
    {"API", "api"}
  ]

  attr :conversation_count, :integer, required: true
  attr :status, :string, required: true, doc: "`active` or `archived` — default route is active."
  attr :is_admin, :boolean, required: true
  attr :filter_scope, :string, required: true
  attr :filter_channel_type, :string, required: true
  attr :filter_team_id, :string, required: true
  attr :filter_person_id, :string, required: true
  attr :teams, :list, required: true
  attr :people, :list, required: true

  def conversation_filters(assigns) do
    assigns =
      assigns
      |> assign(:count_suffix, "#{assigns.conversation_count} conversations")
      |> assign(:channel_type_options, @channel_type_options)

    ~H"""
    <div class="zaq-ingestion-chrome-row zaq-ingestion-chrome-row--spaced">
      <DSToggle.toggle
        :if={@is_admin}
        value={@filter_scope}
        event="filter"
        value_param="scope"
        suffix={@count_suffix}
        choices={[
          %{value: "own", label: "My History"},
          %{value: "all", label: "All Users"}
        ]}
      />
      <span :if={not @is_admin} class="zaq-text-caption zaq-toggle-count">
        {@count_suffix}
      </span>

      <form
        phx-change="filter"
        class="zaq-ingestion-chrome-actions zaq-ingestion-chrome-actions--end flex flex-wrap items-center gap-3"
      >
        <div :if={@is_admin && @filter_scope == "all"} class="flex items-center gap-1.5">
          <label class="font-mono text-[0.7rem] text-black/40 uppercase tracking-wider whitespace-nowrap">
            Team
          </label>
          <div class="w-40">
            <.searchable_select
              id="filter-team"
              name="team_id"
              value={@filter_team_id}
              placeholder="Search team..."
              empty_label="All teams"
              compact={true}
              options={[{"All teams", "all"} | Enum.map(@teams, &{&1.name, &1.id})]}
            />
          </div>
        </div>

        <div :if={@is_admin && @filter_scope == "all"} class="flex items-center gap-1.5">
          <label class="font-mono text-[0.7rem] text-black/40 uppercase tracking-wider whitespace-nowrap">
            Person
          </label>
          <div class="w-44">
            <.searchable_select
              id="filter-person"
              name="person_id"
              value={@filter_person_id}
              placeholder="Search person..."
              empty_label="All people"
              compact={true}
              on_search="search_people"
              options={[{"All people", "all"} | Enum.map(@people, &{&1.full_name, &1.id})]}
            />
          </div>
        </div>

        <div class="flex items-center gap-1.5">
          <label
            for="history-status-trigger"
            class="font-mono text-[0.7rem] text-black/40 uppercase tracking-wider whitespace-nowrap"
          >
            Status
          </label>
          <div class="w-28">
            <.select
              id="history-status"
              name="status"
              value={@status}
              compact={true}
              options={[{"Active", "active"}, {"Archived", "archived"}]}
              class="mb-0"
            />
          </div>
        </div>

        <div class="flex items-center gap-1.5">
          <label
            for="channel_type-trigger"
            class="font-mono text-[0.7rem] text-black/40 uppercase tracking-wider whitespace-nowrap"
          >
            Channel
          </label>
          <div class="w-32">
            <.select
              id="channel_type"
              name="channel_type"
              value={@filter_channel_type}
              compact={true}
              options={@channel_type_options}
              class="mb-0"
            />
          </div>
        </div>
      </form>
    </div>
    """
  end
end
