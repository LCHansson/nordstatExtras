test_that("opening a v1 database migrates it in place to v2", {
  path <- tempfile(fileext = ".sqlite")
  on.exit(unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE))

  # Hand-build a v1 database: create the tables the old schema had, insert
  # a dummy cells row, mark schema_version = 1. Do NOT create meta_search
  # or the kind/payload columns.
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  DBI::dbExecute(con, "PRAGMA journal_mode = WAL;")
  DBI::dbExecute(con, "
    CREATE TABLE nxt_meta (key TEXT PRIMARY KEY, value TEXT);")
  DBI::dbExecute(con, "
    CREATE TABLE cells (
      cell_id INTEGER PRIMARY KEY AUTOINCREMENT,
      source TEXT NOT NULL,
      api_alias TEXT NOT NULL DEFAULT '',
      entity_id TEXT NOT NULL,
      variable TEXT NOT NULL DEFAULT '',
      period TEXT NOT NULL DEFAULT '',
      dims_hash TEXT NOT NULL,
      value_num REAL, value_txt TEXT, status TEXT,
      lang TEXT NOT NULL DEFAULT '',
      fetched_at INTEGER NOT NULL,
      UNIQUE (source, api_alias, entity_id, variable, period, dims_hash, lang)
    );")
  DBI::dbExecute(con, "
    CREATE TABLE cell_dims (
      dims_hash TEXT NOT NULL, dim_name TEXT NOT NULL,
      dim_code TEXT, dim_label TEXT,
      PRIMARY KEY (dims_hash, dim_name)
    );")
  DBI::dbExecute(con, "
    CREATE TABLE queries (
      query_hash TEXT PRIMARY KEY,
      source TEXT NOT NULL, entity TEXT NOT NULL,
      fetched_at INTEGER NOT NULL,
      n_expected INTEGER NOT NULL DEFAULT 0,
      df_template TEXT
    );")
  DBI::dbExecute(con, "
    CREATE TABLE query_cells (
      query_hash TEXT NOT NULL, cell_id INTEGER NOT NULL,
      PRIMARY KEY (query_hash, cell_id)
    );")
  DBI::dbExecute(con,
    "INSERT INTO cells (source, entity_id, dims_hash, value_num, fetched_at)
     VALUES ('kolada', 'N03700', 'abc', 42.0, ?);",
    params = list(as.integer(Sys.time())))
  DBI::dbExecute(con,
    "INSERT INTO nxt_meta (key, value) VALUES ('schema_version', '1');")
  DBI::dbDisconnect(con)

  # Now open with nxt_open — should migrate straight to current (v3)
  handle <- nxt_open(path)
  on.exit(nxt_close(handle), add = TRUE, after = FALSE)

  # schema_version is now at current version
  ver <- DBI::dbGetQuery(handle$con,
    "SELECT value FROM nxt_meta WHERE key = 'schema_version';")$value
  expect_equal(ver, as.character(nordstatExtras:::NXT_SCHEMA_VERSION))

  # queries has kind + payload columns
  cols <- DBI::dbGetQuery(handle$con, "PRAGMA table_info(queries);")$name
  expect_true("kind" %in% cols)
  expect_true("payload" %in% cols)

  # meta_search table exists
  expect_true("meta_search" %in% DBI::dbListTables(handle$con))

  # Pre-existing cells row is still there
  n <- DBI::dbGetQuery(handle$con,
    "SELECT COUNT(*) AS n FROM cells;")$n
  expect_equal(n, 1L)
})

test_that("migration is idempotent (nxt_open on already-v2 DB is a no-op)", {
  path <- tempfile(fileext = ".sqlite")
  on.exit(unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE))

  h1 <- nxt_open(path)
  nxt_close(h1)
  h2 <- nxt_open(path)
  on.exit(nxt_close(h2), add = TRUE, after = FALSE)

  ver <- DBI::dbGetQuery(h2$con,
    "SELECT value FROM nxt_meta WHERE key = 'schema_version';")$value
  expect_equal(ver, as.character(nordstatExtras:::NXT_SCHEMA_VERSION))
})
