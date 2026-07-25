# UX Plan: Ingestion Jobs Drawer

## JTBD

Browse and ingest files at full width while monitoring job progress on demand, without table/jobs panel overlap on responsive layouts.

## Primary users

BO admins managing local or provider-backed ingestion volumes.

## IA change

- **Before:** `lg:grid-cols-3` split — file browser (2/3) + jobs panel (1/3).
- **After:** Full-width file browser; jobs live in a right drawer opened via toolbar.

## User flow

```
Flow: Monitor ingestion jobs
Actor: BO admin
Trigger: Click "Monitor jobs" (or auto-open after ingest)
Goal: View/filter/retry/cancel jobs without leaving file browser

Steps:
1. Ingestion page → full-width file list/grid
2. Click "Monitor jobs" → right drawer (1/3 desktop, full-width mobile)
3. Filter / retry / cancel as today → close drawer → focus returns to button

Alternate:
- After "Ingest Selected" succeeds → drawer auto-opens

Failure:
- Drawer closed + active jobs → button shows active count suffix
```

## Screen: Ingestion (updated)

**Route:** `/bo/ingestion`

### Layout zones

```
┌──────────────────────────────────────────────────────────────┐
│ Page header: Ingestion                                       │
├──────────────────────────────────────────────────────────────┤
│ Volume selector (if applicable)                              │
├──────────────────────────────────────────────────────────────┤
│ Chrome row: [list/grid toggle] [toolbar actions…]            │
│   … Upload | New Folder | … | Monitor jobs | Ingest Selected │
├──────────────────────────────────────────────────────────────┤
│ Breadcrumb + file table / grid (full width)                  │
└──────────────────────────────────────────────────────────────┘

Drawer (overlay, right):
┌──────────────── Jobs ──────── ✕ ┐
│ Status filter toggle            │
│ Job cards (scroll)              │
└─────────────────────────────────┘
```

## Component mapping

| UX need | Module | Gap? |
|---------|--------|------|
| Page shell | `BOLayout.bo_layout` | — |
| Jobs drawer | `Drawer.drawer/1` | responsive width `[GAP]` via CSS |
| Jobs content | `IngestionJobsPanel.jobs_panel/1` | remove inner max-height when in drawer |
| Monitor trigger | `IngestionFileBrowserHeader` + `DesignSystem.Button` | — |
| File table | `IngestionFileListView` | — |

## UX decisions

- Drawer `placement: :right`, `size: :one_third` on desktop; full viewport width below `lg`.
- Auto-open drawer after successful ingest selected.
- Button shows `(N active)` when pending/processing jobs exist.
- `return_focus_id` → `#monitor-jobs-button` on close.

## UI Designer Brief

### Build order

1. Drawer shell + LiveView state
2. Monitor jobs button + active count
3. Remove grid split; full-width file browser
4. Responsive drawer CSS

### Prototype handoff

Production implementation on existing `IngestionLive` (not a staged prototype).
