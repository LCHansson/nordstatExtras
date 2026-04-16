# nordstatExtras 0.1.0.9002 (development)

## New features

* **Schema v3: `search_keywords` column + 3-column FTS5 index.**
  `meta_search` gains a `search_keywords TEXT` column (default `''`)
  that participates in the FTS5 index alongside `title` and
  `description`. Existing v2 databases are migrated on first
  `nxt_open()` — the column is added via `ALTER TABLE` and the FTS5
  virtual table is rebuilt to cover all three columns.
* **`recorded_searches` table.** One row per `(query, source)` pair
  that has been run against a live source API via the "nödutgång"
  live-search flow. Used by cron to refresh entity-search-keyword
  associations before they go stale. Schema: `query, source,
  last_run_at, ttl_days, last_hit_count` (PK on `query, source`).
* **`nxt_record_search(handle, query, source, entity_ids, ttl_days = 30L)`.**
  Appends the query token to every listed entity's `search_keywords`
  (deduplicated), refreshes the FTS5 index, and UPSERTs the query into
  `recorded_searches`. Enables local search to find entities a
  server-side live search surfaced — critical for PX-Web where the
  remote search matches fields the local index doesn't mirror.
* **`nxt_expired_searches(handle, max_age = 30)`.** Returns recorded
  searches whose age exceeds `MIN(ttl_days, max_age)`, ordered
  oldest-first. Cron loops over this list, re-runs each live search,
  then calls `nxt_record_search()` with the fresh entity ids.

# nordstatExtras 0.1.0.9001 (development)

## Bug fixes

* `normalize_kolada()` and `reconstruct_kolada()` now accept both
  `year` (produced by `rKolada::get_values(simplify = TRUE)`) and
  `period` (produced by `simplify = FALSE`). Previously the cache
  path aborted with `"missing required columns: year"` whenever the
  user combined `simplify = FALSE` with an `nxt_handle`-backed cache.
  The period column is now resolved dynamically and the original name
  is round-tripped back out, so both simplify modes cache and
  reconstruct correctly.

# nordstatExtras 0.1.0

Initial CRAN release.

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
