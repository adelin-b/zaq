defmodule ZaqWeb.Components.DesignSystem.BreadcrumbTest do
  use ZaqWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  alias ZaqWeb.Components.DesignSystem.Breadcrumb

  test "breadcrumb/1 is hidden at root with an empty trail" do
    html =
      render_component(&Breadcrumb.breadcrumb/1,
        current_dir: ".",
        breadcrumbs: []
      )

    refute html =~ "zaq-breadcrumb-row"
    refute html =~ "root"
  end

  test "breadcrumb/1 shows root and trail when inside a folder" do
    html =
      render_component(&Breadcrumb.breadcrumb/1,
        current_dir: "docs/sub",
        breadcrumbs: [
          %{name: "docs", path: "docs"},
          %{name: "sub", path: "docs/sub"}
        ]
      )

    assert html =~ "zaq-breadcrumb-row"
    assert html =~ "root"
    assert html =~ "docs"
    assert html =~ "sub"
    assert html =~ "zaq-breadcrumb-back-btn"
  end

  test "page_breadcrumb/1 renders link and current crumbs" do
    html =
      render_component(&Breadcrumb.page_breadcrumb/1,
        items: [
          %{label: "Workflows", to: "/bo/workflows"},
          %{label: "Lead intake", current: true}
        ]
      )

    assert html =~ "zaq-breadcrumb-row"
    assert html =~ "aria-label=\"Breadcrumb\""
    assert html =~ ~s(href="/bo/workflows")
    assert html =~ "Workflows"
    assert html =~ "Lead intake"
    assert html =~ "zaq-breadcrumb-current"
    assert html =~ "zaq-breadcrumb-sep"
  end

  test "page_breadcrumb/1 renders all intermediate links" do
    html =
      render_component(&Breadcrumb.page_breadcrumb/1,
        items: [
          %{label: "Workflows", to: "/bo/workflows"},
          %{label: "Lead intake", to: "/bo/workflows/abc"},
          %{label: "Run abc12345", current: true}
        ]
      )

    assert html =~ ~s(href="/bo/workflows")
    assert html =~ ~s(href="/bo/workflows/abc")
    assert html =~ "Run abc12345"
  end

  test "page_breadcrumb/1 renders nothing when items is empty" do
    html = render_component(&Breadcrumb.page_breadcrumb/1, items: [])

    refute html =~ "zaq-breadcrumb-row"
  end
end
