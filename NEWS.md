# nordstatExtras 0.0.0.9000

Initial development version.

## Data caching (phases 0–5)

* SQLite-backed cell-level cache for the nordstat family (rKolada, rTrafa,
  pixieweb). Designed for multi-user web applications with concurrent reads
  and writes.
* Gatekeeper schema (`cells`) with sidecar `cell_dims` for arbitrary
  metadata dimensions.
* Query → cells junction (`query_cells`) preserves the drop-in closure
  semantics used by the source packages.
* Cell-level TTL — a query inherits freshness from any other query that
  refreshed overlapping cells. Default 30 days, configurable.
* Optional asynchronous writes via `mirai`, with graceful synchronous
  fallback when `mirai` is not installed.
* Integration hooks in rKolada `get_values()`, rTrafa `get_data()`, and
  pixieweb `get_data()` — opt-in via `cache = TRUE` + a `.sqlite`
  cache_location or an `nxt_handle`.

## Metadata caching + search (phase 6)

* Schema v2 — `queries.kind` (`data` | `metadata`) + `queries.payload`
  (BLOB) extend the existing table via a version-guarded migration.
* `nxt_cache_handler(kind = "metadata")` stores whole objects (tibbles or
  lists) as serialized BLOBs. Lossless: preserves attributes like
  `pixieweb`'s `px_api`, handles `rTrafa::get_structure_raw()` which
  returns a list, and roundtrips nested list columns (`kpi_groups.members`
  etc.) byte-identically.
* `nxt_search()` — FTS5-powered typeahead across cached metadata. Search
  extractors per source (`R/search.R`) populate a lossy `meta_search`
  side-index as a write side effect. Sub-millisecond matches on thousands
  of rows. Falls back to LIKE prefix match when FTS5 is unavailable.
* Integration hooks in `rKolada::get_metadata()`, `rTrafa::get_products()`,
  `rTrafa::get_structure_raw()`, and `pixieweb::get_tables()`.
* `pixieweb::table_enrich()` — **per-table cache granularity**. Each
  enriched table is stored under its own `query_hash`, so overlapping
  `table_enrich()` calls share rows. `async = TRUE` launches a `mirai`
  background job and returns the currently-cached subset plus a
  `nxt_promise` attribute that apps can bridge to `promises::then()` for
  Shiny integration. Resume-on-crash is automatic.
