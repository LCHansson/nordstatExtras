# SQLite schema for the nordstatExtras cache.
#
# Design:
#   * `cells`           — one row per statistical datum, with UNIQUE
#                         constraint enabling cell-level deduplication across
#                         queries. Each cell carries its own `fetched_at` —
#                         this is the TTL source of truth for kind='data';
#                         see `nxt_lookup_query()`.
#   * `cell_dims`       — sidecar table for arbitrary dimensions beyond the
#                         gatekeeper columns. One row per (dims_hash, dim_name).
#   * `queries`         — per-query metadata. For kind='data': df_template for
#                         rebuild, n_expected for tamper/GC detection. For
#                         kind='metadata': serialized payload BLOB is the
#                         entire cached result.
#   * `query_cells`     — junction mapping data query → cells it served.
#   * `meta_search`     — lossy search index populated as side effect of
#                         metadata stores. Extracts (entity_id, title,
#                         description) from cached metadata rows.
#   * `meta_search_fts` — FTS5 virtual table over `meta_search`. Powers
#                         sub-ms typeahead search in `nxt_search()`.
#   * `nxt_meta`        — schema version + operational metadata.
#
# TTL semantics:
#   kind='data'     — cell-level: fresh iff every expected cell is still
#                     present (n_present == n_expected) AND the oldest cell
#                     is younger than max_age. Cross-query freshness
#                     propagates via cells UPSERT.
#   kind='metadata' — query-level: fresh iff queries.fetched_at is younger
#                     than max_age. No cross-query reuse on opaque blobs.

NXT_SCHEMA_VERSION <- 3L

# Base DDL applied to fresh databases. Idempotent via IF NOT EXISTS.
# Upgrade migrations for existing v1 databases are handled separately in
# nxt_apply_schema() below.
nxt_schema_ddl <- function() {
  c(
    "CREATE TABLE IF NOT EXISTS nxt_meta (
       key TEXT PRIMARY KEY,
       value TEXT
     );",

    "CREATE TABLE IF NOT EXISTS cells (
       cell_id     INTEGER PRIMARY KEY AUTOINCREMENT,
       source      TEXT NOT NULL,
       api_alias   TEXT NOT NULL DEFAULT '',
       entity_id   TEXT NOT NULL,
       variable    TEXT NOT NULL DEFAULT '',
       period      TEXT NOT NULL DEFAULT '',
       dims_hash   TEXT NOT NULL,
       value_num   REAL,
       value_txt   TEXT,
       status      TEXT,
       lang        TEXT NOT NULL DEFAULT '',
       fetched_at  INTEGER NOT NULL,
       UNIQUE (source, api_alias, entity_id, variable, period, dims_hash, lang)
     );",

    "CREATE INDEX IF NOT EXISTS idx_cells_lookup
       ON cells (source, entity_id, period);",

    "CREATE TABLE IF NOT EXISTS cell_dims (
       dims_hash TEXT NOT NULL,
       dim_name  TEXT NOT NULL,
       dim_code  TEXT,
       dim_label TEXT,
       PRIMARY KEY (dims_hash, dim_name)
     );",

    "CREATE TABLE IF NOT EXISTS queries (
       query_hash  TEXT PRIMARY KEY,
       source      TEXT NOT NULL,
       entity      TEXT NOT NULL,
       kind        TEXT NOT NULL DEFAULT 'data',
       fetched_at  INTEGER NOT NULL,
       n_expected  INTEGER NOT NULL DEFAULT 0,
       df_template TEXT,
       payload     BLOB
     );",

    "CREATE INDEX IF NOT EXISTS idx_queries_stale ON queries (fetched_at);",

    "CREATE TABLE IF NOT EXISTS query_cells (
       query_hash TEXT NOT NULL,
       cell_id    INTEGER NOT NULL,
       PRIMARY KEY (query_hash, cell_id)
     );",

    "CREATE INDEX IF NOT EXISTS idx_query_cells_cell ON query_cells (cell_id);",

    "CREATE TABLE IF NOT EXISTS meta_search (
       source          TEXT NOT NULL,
       entity_type     TEXT NOT NULL,
       entity_id       TEXT NOT NULL,
       title           TEXT,
       description     TEXT,
       search_keywords TEXT NOT NULL DEFAULT '',
       query_hash      TEXT,
       PRIMARY KEY (source, entity_type, entity_id)
     );",

    "CREATE INDEX IF NOT EXISTS idx_meta_search_source
       ON meta_search (source, entity_type);",

    "CREATE INDEX IF NOT EXISTS idx_meta_search_query
       ON meta_search (query_hash);",

    # recorded_searches — one row per (query, source) combination that has
    # ever been run against a live source API via the "nödutgång" live-search
    # flow. Cron re-runs these to keep per-entity search_keywords fresh even
    # when entities' titles/descriptions don't contain the search term
    # (critical for pxweb, whose server-side search matches more than the
    # visible text — tables like 'Antal bussar' match 'sjoefart').
    "CREATE TABLE IF NOT EXISTS recorded_searches (
       query        TEXT NOT NULL,
       source       TEXT NOT NULL,
       last_run_at  INTEGER NOT NULL,
       ttl_days     INTEGER NOT NULL DEFAULT 30,
       last_hit_count INTEGER NOT NULL DEFAULT 0,
       PRIMARY KEY (query, source)
     );",

    "CREATE INDEX IF NOT EXISTS idx_recorded_searches_run
       ON recorded_searches (last_run_at);"
  )
}

# Check whether SQLite was compiled with FTS5. Used both for schema setup
# (create the virtual table conditionally) and by nxt_search() to pick
# between FTS5 match and a LIKE fallback.
nxt_has_fts5 <- function(con) {
  opts <- tryCatch(
    DBI::dbGetQuery(con, "PRAGMA compile_options;")$compile_options,
    error = function(e) character()
  )
  any(grepl("^ENABLE_FTS5$", opts))
}

# Read the current schema_version from nxt_meta, or return 0 for DBs that
# don't have the row yet (a truly fresh database).
get_schema_version <- function(con) {
  row <- tryCatch(
    DBI::dbGetQuery(con,
      "SELECT value FROM nxt_meta WHERE key = 'schema_version';"),
    error = function(e) NULL
  )
  if (is.null(row) || nrow(row) == 0) return(0L)
  as.integer(row$value[1])
}

# Apply schema to a fresh or existing connection. Idempotent — safe to call
# on every nxt_open(). Runs version-guarded migration for v1 → v2.
nxt_apply_schema <- function(con) {
  DBI::dbExecute(con, "PRAGMA journal_mode = WAL;")
  DBI::dbExecute(con, "PRAGMA synchronous = NORMAL;")
  DBI::dbExecute(con, "PRAGMA foreign_keys = ON;")

  # Apply base DDL first. For a v1 database this is a no-op (IF NOT EXISTS
  # guards everything) except it creates the new meta_search table.
  # For a fresh database this creates everything at v2 shape.
  for (stmt in nxt_schema_ddl()) {
    DBI::dbExecute(con, stmt)
  }

  current <- get_schema_version(con)

  # v1 databases are missing the `kind` and `payload` columns on queries.
  # ALTER TABLE ADD COLUMN is not idempotent, so guard with PRAGMA lookup.
  if (current < 2L) {
    queries_cols <- DBI::dbGetQuery(con, "PRAGMA table_info(queries);")$name
    if (!"kind" %in% queries_cols) {
      DBI::dbExecute(
        con,
        "ALTER TABLE queries ADD COLUMN kind TEXT NOT NULL DEFAULT 'data';"
      )
    }
    if (!"payload" %in% queries_cols) {
      DBI::dbExecute(con, "ALTER TABLE queries ADD COLUMN payload BLOB;")
    }
  }

  # v2 → v3 migration: add `search_keywords` column to meta_search, rebuild
  # the FTS5 index over three columns (title, description, search_keywords).
  # Existing rows get an empty-string default so the schema change is safe
  # even while the app is running against the same database.
  needs_fts_rebuild <- FALSE
  if (current < 3L) {
    ms_cols <- DBI::dbGetQuery(con, "PRAGMA table_info(meta_search);")$name
    if (!"search_keywords" %in% ms_cols) {
      DBI::dbExecute(
        con,
        "ALTER TABLE meta_search ADD COLUMN search_keywords TEXT NOT NULL DEFAULT '';"
      )
      needs_fts_rebuild <- TRUE
    }
  }

  # Index on queries.kind — must come AFTER the ALTER TABLE migration since
  # v1 databases don't yet have the column when the base DDL runs.
  DBI::dbExecute(
    con,
    "CREATE INDEX IF NOT EXISTS idx_queries_kind ON queries (kind);"
  )

  # FTS5 virtual table — conditional on SQLite having FTS5 compiled in.
  # nxt_search() has a LIKE fallback for databases that don't. The v2-era
  # index covered (title, description); v3 adds search_keywords. When
  # upgrading a v2 DB we drop + recreate to pick up the new column. Fresh
  # v3 DBs create the three-column index directly.
  if (nxt_has_fts5(con)) {
    if (needs_fts_rebuild) {
      DBI::dbExecute(con, "DROP TABLE IF EXISTS meta_search_fts;")
    }
    DBI::dbExecute(
      con,
      "CREATE VIRTUAL TABLE IF NOT EXISTS meta_search_fts USING fts5(
         title, description, search_keywords,
         content='meta_search',
         content_rowid='rowid',
         tokenize='unicode61 remove_diacritics 2'
       );"
    )
    # Repopulate from existing meta_search rows after a rebuild. (Noop when
    # creating fresh since meta_search is empty.)
    if (needs_fts_rebuild) {
      DBI::dbExecute(
        con,
        "INSERT INTO meta_search_fts(rowid, title, description, search_keywords)
           SELECT rowid, title, description, search_keywords FROM meta_search;"
      )
    }
  }

  # Stamp the current version. INSERT OR REPLACE handles both fresh DBs
  # and migrated-from-v1 DBs (where the row said '1').
  DBI::dbExecute(
    con,
    "INSERT INTO nxt_meta (key, value) VALUES ('schema_version', ?)
     ON CONFLICT(key) DO UPDATE SET value = excluded.value;",
    params = list(as.character(NXT_SCHEMA_VERSION))
  )
  invisible(con)
}
