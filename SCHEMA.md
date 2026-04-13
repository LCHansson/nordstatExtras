# nordstatExtras SQLite schema reference

Schema version: **2**
(Read from `SELECT value FROM nxt_meta WHERE key = 'schema_version'`.)

A fresh `nxt_open()` creates **12 tables** in the SQLite file. Only 7 of
them are user-facing; the other 5 are managed automatically by SQLite (FTS5
shadow tables + the `AUTOINCREMENT` counter) and should be treated as
opaque internals.

```
User-facing                        SQLite-managed
-----------                        --------------
nxt_meta                           sqlite_sequence       (AUTOINCREMENT bookkeeping)
cells                              meta_search_fts_data    \
cell_dims                          meta_search_fts_idx     |  FTS5 external-content
queries                            meta_search_fts_docsize |  shadow tables — DO NOT
query_cells                        meta_search_fts_config  /  touch directly
meta_search
meta_search_fts                    (virtual table — FTS5 over meta_search)
```

## Conceptual model

Two kinds of content live side-by-side in the same cache:

```
                    ┌───────────────────────────────┐
                    │         queries               │
                    │  one row per cached query     │
                    │  kind = 'data' | 'metadata'   │
                    └───────┬─────────────┬─────────┘
                            │             │
              kind='data'   │             │  kind='metadata'
                            ▼             ▼
              ┌─────────────────┐   ┌────────────────┐
              │   query_cells   │   │ queries.payload│
              │  (junction)     │   │   (BLOB)       │
              └────────┬────────┘   └────────────────┘
                       │
                       ▼
              ┌─────────────────┐   ┌────────────────┐
              │     cells       │◄──┤   cell_dims    │
              │ one row per     │   │ arbitrary dims │
              │ statistical     │   │ per dims_hash  │
              │ value           │   └────────────────┘
              └─────────────────┘

                            ┌───────────────────────┐
  Side index populated as   │      meta_search      │   (base table,
  a write-side effect of    │   lossy view over     │   plain SQLite)
  every nxt_store_meta():   │   cached metadata     │
                            └───────────┬───────────┘
                                        │
                                        ▼
                            ┌───────────────────────┐
                            │    meta_search_fts    │   (FTS5 virtual,
                            │ tokenize unicode61,   │   content-linked to
                            │ diacritics-insensitive│   meta_search)
                            └───────────────────────┘
```

Every cached query gets one row in `queries`. That row's `kind` determines
where the data lives:

- `kind = 'data'` — rows spread across `cells` (via `query_cells`
  junction). Lets overlapping queries share cells and inherit freshness
  from each other.
- `kind = 'metadata'` — the whole serialized result sits in
  `queries.payload` as a BLOB. No decomposition, no junction table. If
  the metadata entity is searchable (registered in
  `META_SEARCH_EXTRACTORS`) a lossy projection is also written to
  `meta_search` for typeahead.

## User-facing tables

### `nxt_meta`

Key/value metadata about the database itself.

| column | type | notes |
|---|---|---|
| `key`   | TEXT PRIMARY KEY | e.g. `'schema_version'` |
| `value` | TEXT             | e.g. `'2'` |

Only the `schema_version` row is written by the package today. Future
migrations will stamp additional keys.

### `cells`

One row per statistical datum. This is the `kind = 'data'` storage; the
gatekeeper columns are the ones every source package can populate. Any
dimensions outside the gatekeeper set live in `cell_dims` and are linked
via `dims_hash`.

| column | type | notes |
|---|---|---|
| `cell_id`    | INTEGER PRIMARY KEY AUTOINCREMENT | surrogate key |
| `source`     | TEXT NOT NULL                     | `'kolada'` / `'trafa'` / `'pixieweb'` |
| `api_alias`  | TEXT NOT NULL DEFAULT `''`        | `'scb'`, `'ssb'`, ... for pixieweb; `''` otherwise |
| `entity_id`  | TEXT NOT NULL                     | KPI code, product code, table id, ... |
| `variable`   | TEXT NOT NULL DEFAULT `''`        | measure name (rTrafa) or `ContentsCode` (pixieweb) |
| `period`     | TEXT NOT NULL DEFAULT `''`        | year as text (or `ar` / `Tid` value) |
| `dims_hash`  | TEXT NOT NULL                     | SHA-1 of sorted dimension vector → `cell_dims` |
| `value_num`  | REAL                              | numeric value, or NULL |
| `value_txt`  | TEXT                              | text fallback for non-numeric values |
| `status`     | TEXT                              | source-specific status flag (Kolada's `status` column) |
| `lang`       | TEXT NOT NULL DEFAULT `''`        | `'sv'`, `'en'`, `'SV'`, ... |
| `fetched_at` | INTEGER NOT NULL                  | unix seconds — **TTL source of truth** |

**Constraints / indexes**
- `UNIQUE (source, api_alias, entity_id, variable, period, dims_hash, lang)`
  — the dedup key. Enables cross-query cell reuse: overlapping queries
  UPSERT the same row.
- `INDEX idx_cells_lookup (source, entity_id, period)` — common lookup path.

Empty-string sentinels (`''`) are used instead of NULL for the
UNIQUE-significant columns because SQLite treats NULLs as distinct in
UNIQUE constraints (which would defeat dedup).

### `cell_dims`

Sidecar for arbitrary dimensions beyond the gatekeeper columns. One row
per `(dims_hash, dim_name)`. Lets rTrafa and pixieweb carry their
package-specific dimensions (e.g. `drivm`, `Region`, `Civilstand`)
without making `cells` sparse.

| column | type | notes |
|---|---|---|
| `dims_hash` | TEXT NOT NULL | → `cells.dims_hash` |
| `dim_name`  | TEXT NOT NULL | e.g. `'drivm'`, `'Region'` |
| `dim_code`  | TEXT          | raw code (`'102'`) |
| `dim_label` | TEXT          | human label (`'Diesel'`); may be NULL |

**Primary key:** `(dims_hash, dim_name)`. Idempotent UPSERTs.

### `queries`

One row per *cached query*, across both kinds. This is the entry point
for `discover` / `load` / `store` in `nxt_cache_handler()`.

| column | type | notes |
|---|---|---|
| `query_hash`  | TEXT PRIMARY KEY       | SHA-1 of `(source, entity, sorted key_params)` |
| `source`      | TEXT NOT NULL          | same enum as `cells.source` |
| `entity`      | TEXT NOT NULL          | `'values'`, `'kpi'`, `'products'`, `'tables'`, `'enriched_row'`, ... |
| `kind`        | TEXT NOT NULL DEFAULT `'data'` | `'data'` or `'metadata'` |
| `fetched_at`  | INTEGER NOT NULL       | last write — **TTL for `kind='metadata'`** |
| `n_expected`  | INTEGER NOT NULL DEFAULT 0 | for `kind='data'`: number of cells the query originally produced (used to detect GC'd cells in discover) |
| `df_template` | TEXT                   | JSON blob with column order/types for `reconstruct_*()`. Only set when `kind='data'` |
| `payload`     | BLOB                   | `base::serialize()` of the full metadata result. Only set when `kind='metadata'` |

**Indexes**
- `INDEX idx_queries_stale (fetched_at)` — scan for GC candidates
- `INDEX idx_queries_kind (kind)` — kind-aware cleanup in `nxt_clear` / `nxt_gc`

### `query_cells`

Junction: which cells belong to which `kind='data'` query. Empty for
`kind='metadata'` queries.

| column | type | notes |
|---|---|---|
| `query_hash` | TEXT NOT NULL    | → `queries.query_hash` |
| `cell_id`    | INTEGER NOT NULL | → `cells.cell_id` |

**Primary key:** `(query_hash, cell_id)`.
**Index:** `idx_query_cells_cell (cell_id)` — speeds up orphan cleanup.

### `meta_search`

Lossy projection of cached metadata for typeahead search. Populated as a
write side effect of every `nxt_store_meta()` call whose
`(source, entity)` has an extractor registered in `META_SEARCH_EXTRACTORS`
(see `R/search.R`). Not every metadata entity lands here — filter-ish
entities like `municipality`, `ou`, `kpi_groups`, and `structure` are
deliberately excluded so they don't clutter search results.

| column | type | notes |
|---|---|---|
| `source`      | TEXT NOT NULL | same enum |
| `entity_type` | TEXT NOT NULL | `'kpi'`, `'products'`, `'tables'`, `'enriched'`, `'enriched_row'`, `'measures'` |
| `entity_id`   | TEXT NOT NULL | KPI code / product name / table id / `product\x1fmeasure` for composites |
| `title`       | TEXT          | primary searchable string |
| `description` | TEXT          | secondary searchable string (may be NULL) |
| `query_hash`  | TEXT          | the query that last wrote this row — enables cascade-delete on clear/gc |

**Primary key:** `(source, entity_type, entity_id)`.
**Indexes:** `idx_meta_search_source (source, entity_type)`, `idx_meta_search_query (query_hash)`.

### `meta_search_fts` (FTS5 virtual table)

External-content FTS5 index over `meta_search`. This is what
`nxt_search()` actually queries.

- Indexed columns: `title`, `description`
- Tokenizer: `unicode61 remove_diacritics 2` — case-insensitive, Swedish
  å/ä/ö and é/ü fold to their base characters, so `ar` matches `År` and
  `invanare` matches `invånare`.
- `content='meta_search'` — the FTS table doesn't duplicate the payload,
  it links to the base table via `rowid`. We manually `INSERT INTO
  meta_search_fts(meta_search_fts) VALUES('rebuild')` after writes to
  keep the index synchronized (external-content tables don't auto-sync
  without triggers, and we opted for explicit rebuild for simplicity).

Query pattern used by `nxt_search()`:

```sql
SELECT ms.source, ms.entity_type, ms.entity_id,
       ms.title, ms.description, meta_search_fts.rank
  FROM meta_search_fts
  JOIN meta_search ms ON ms.rowid = meta_search_fts.rowid
 WHERE meta_search_fts MATCH ?              -- user's query
   AND (? IS NULL OR ms.source IN (...))    -- optional filters
   AND (? IS NULL OR ms.entity_type IN (...))
 ORDER BY meta_search_fts.rank              -- lower = better
 LIMIT ?;
```

Supports prefix matches (`bef*`), phrase matches (`"exakt fras"`), and
boolean operators (`AND` / `OR` / `NOT`).

## SQLite-managed tables (ignore)

These are created automatically by SQLite when the main schema is
applied. You should never read or write to them directly — modifying them
will corrupt the FTS5 index or the AUTOINCREMENT counter.

- `sqlite_sequence` — internal bookkeeping for `cells.cell_id` AUTOINCREMENT
- `meta_search_fts_data` — FTS5's inverted-index segments
- `meta_search_fts_idx` — FTS5's segment directory
- `meta_search_fts_docsize` — per-row length stats used for ranking
- `meta_search_fts_config` — FTS5 compile-time options snapshot

## TTL semantics

Both kinds use the same `max_age_days` argument (default 30), but resolve
freshness differently:

**`kind='data'` — cell-level TTL.** A query is fresh iff
1. every expected cell is still present (`n_present == n_expected`), and
2. the *oldest* cell is younger than `max_age_days`.

If any other query refreshes overlapping cells via UPSERT, the first
query's `cells.fetched_at` advances automatically — so it inherits the
freshness. This is the point of cell-level storage.

**`kind='metadata'` — query-level TTL.** A metadata blob is fresh iff
`queries.fetched_at` is younger than `max_age_days`. No cross-query reuse
is possible on opaque blobs, so query-level is the natural choice.

`nxt_gc()` branches accordingly: it deletes stale `cells` rows (then
cascades into orphan queries) for the data path, and deletes stale
`queries` rows directly for the metadata path. `meta_search` orphans are
cleaned up after both.

## Inspection recipes

### What's in the cache?

```sql
SELECT kind, source, entity, COUNT(*) AS n_queries,
       datetime(MIN(fetched_at), 'unixepoch') AS oldest,
       datetime(MAX(fetched_at), 'unixepoch') AS newest
  FROM queries
 GROUP BY kind, source, entity
 ORDER BY kind, source, entity;
```

### How many cells per source?

```sql
SELECT source, COUNT(*) AS n_cells
  FROM cells
 GROUP BY source
 ORDER BY source;
```

### Which queries reference the same cells (dedup benefit)?

```sql
SELECT cell_id, COUNT(DISTINCT query_hash) AS n_queries
  FROM query_cells
 GROUP BY cell_id
HAVING n_queries > 1
 ORDER BY n_queries DESC
 LIMIT 10;
```

### What's indexed for search?

```sql
SELECT source, entity_type, COUNT(*) AS n
  FROM meta_search
 GROUP BY source, entity_type
 ORDER BY source, entity_type;
```

### Current schema version

```sql
SELECT value FROM nxt_meta WHERE key = 'schema_version';
```

### Database size breakdown (approximate)

```sql
SELECT name,
       SUM(pgsize) AS bytes
  FROM dbstat
 WHERE name NOT LIKE 'sqlite_%'
 GROUP BY name
 ORDER BY bytes DESC;
```

(`dbstat` requires SQLite to be compiled with `SQLITE_ENABLE_DBSTAT_VTAB`;
RSQLite's binary has it on most platforms.)

## Schema evolution

Migrations are version-guarded and idempotent. `nxt_apply_schema()` runs
on every `nxt_open()` and will bring a v1 database up to v2 in place
(ALTER TABLE ADD COLUMN for `kind` and `payload`, CREATE TABLE for
`meta_search` + `meta_search_fts`).

To add a new schema version:

1. Bump `NXT_SCHEMA_VERSION` in `R/schema.R`.
2. Add the new tables/columns to `nxt_schema_ddl()` (idempotent via
   `IF NOT EXISTS`).
3. Add a `if (current < N)` branch in `nxt_apply_schema()` for any
   non-idempotent ALTER statements.
4. Write a migration test in `tests/testthat/test-schema-migration.R`
   that hand-builds an older-version DB, opens it, and asserts the
   post-migration shape.
