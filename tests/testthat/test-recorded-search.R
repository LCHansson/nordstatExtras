# Tests for nxt_record_search() / nxt_expired_searches()

test_that("nxt_record_search upserts into recorded_searches", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(path) }, add = TRUE)

  # Prime meta_search with two entities so the keyword update can find them.
  ch <- nxt_cache_handler(
    source = "pixieweb", entity = "tables", cache = TRUE,
    cache_location = handle, kind = "metadata",
    key_params = list(q = "seed")
  )
  ch("store", data.frame(
    id = c("TAB1", "TAB2"),
    title = c("Arbetslöshet", "Bussar"),
    description = c(NA, NA),
    stringsAsFactors = FALSE
  ))

  n <- nxt_record_search(handle, query = "punktlighet",
                         source = "pixieweb",
                         entity_ids = c("TAB1", "TAB2"))
  expect_equal(n, 2L)

  rows <- DBI::dbGetQuery(handle$con,
    "SELECT query, source, ttl_days, last_hit_count FROM recorded_searches;")
  expect_equal(nrow(rows), 1)
  expect_equal(rows$query, "punktlighet")
  expect_equal(rows$source, "pixieweb")
  expect_equal(rows$last_hit_count, 2L)

  # The keyword token should now appear in search_keywords
  ms <- DBI::dbGetQuery(handle$con,
    "SELECT entity_id, search_keywords FROM meta_search ORDER BY entity_id;")
  expect_true(all(ms$search_keywords == "punktlighet"))
})

test_that("nxt_record_search deduplicates keyword tokens", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(path) }, add = TRUE)

  ch <- nxt_cache_handler(
    source = "pixieweb", entity = "tables", cache = TRUE,
    cache_location = handle, kind = "metadata",
    key_params = list(q = "seed")
  )
  ch("store", data.frame(id = "TAB1", title = "T",
                         description = NA, stringsAsFactors = FALSE))

  nxt_record_search(handle, "foo", "pixieweb", "TAB1")
  nxt_record_search(handle, "bar", "pixieweb", "TAB1")
  nxt_record_search(handle, "foo", "pixieweb", "TAB1")  # duplicate

  row <- DBI::dbGetQuery(handle$con,
    "SELECT search_keywords FROM meta_search WHERE entity_id = 'TAB1';")
  # Expect exactly two unique tokens, separated by a single space
  toks <- strsplit(row$search_keywords, " ")[[1]]
  expect_setequal(toks, c("foo", "bar"))
})

test_that("nxt_record_search makes the term findable via nxt_search", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(path) }, add = TRUE)

  skip_if_not(nordstatExtras:::nxt_has_fts5(handle$con), "FTS5 required")

  ch <- nxt_cache_handler(
    source = "pixieweb", entity = "tables", cache = TRUE,
    cache_location = handle, kind = "metadata",
    key_params = list(q = "seed")
  )
  ch("store", data.frame(
    id = "TAB1",
    title = "Antal bussar",  # does NOT contain "punktlighet"
    description = "Bussar per lan",
    stringsAsFactors = FALSE
  ))

  # Before recording, no hit for punktlighet
  expect_equal(nrow(nxt_search(handle, "punktlighet*")), 0)

  nxt_record_search(handle, "punktlighet", "pixieweb", "TAB1")

  hits <- nxt_search(handle, "punktlighet*")
  expect_equal(nrow(hits), 1L)
  expect_equal(hits$entity_id, "TAB1")
})

test_that("nxt_expired_searches returns queries older than max_age", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(path) }, add = TRUE)

  # Insert three rows directly with ages 5, 40, 60 days.
  now <- as.integer(Sys.time())
  day <- 86400L
  rows <- data.frame(
    query = c("fresh", "midage", "old"),
    source = c("scb", "scb", "scb"),
    last_run_at = c(now - 5 * day, now - 40 * day, now - 60 * day),
    ttl_days = c(30L, 30L, 30L),
    last_hit_count = c(1L, 2L, 3L)
  )
  DBI::dbWriteTable(handle$con, "recorded_searches", rows,
                    append = TRUE, row.names = FALSE)

  expired <- nxt_expired_searches(handle, max_age = 30L)
  # "fresh" is 5 days old, not expired. "midage" and "old" are past TTL.
  expect_equal(sort(expired$query), c("midage", "old"))
})

test_that("schema migration v2 → v3 adds search_keywords", {
  path <- tempfile(fileext = ".sqlite")

  # Create a v2-shaped database manually (pre-migration shape)
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  DBI::dbExecute(con, "CREATE TABLE nxt_meta (key TEXT PRIMARY KEY, value TEXT);")
  DBI::dbExecute(con, "INSERT INTO nxt_meta VALUES ('schema_version', '2');")
  DBI::dbExecute(con, "CREATE TABLE meta_search (
    source TEXT NOT NULL, entity_type TEXT NOT NULL, entity_id TEXT NOT NULL,
    title TEXT, description TEXT, query_hash TEXT,
    PRIMARY KEY (source, entity_type, entity_id));")
  DBI::dbExecute(con,
    "INSERT INTO meta_search (source, entity_type, entity_id, title)
     VALUES ('pixieweb', 'tables', 'TAB1', 'test');")
  DBI::dbDisconnect(con)

  # nxt_open should run the v2 → v3 migration
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(path) }, add = TRUE)

  cols <- DBI::dbGetQuery(handle$con, "PRAGMA table_info(meta_search);")$name
  expect_true("search_keywords" %in% cols)

  # Existing row should have empty-string keywords after the migration
  row <- DBI::dbGetQuery(handle$con,
    "SELECT search_keywords FROM meta_search WHERE entity_id = 'TAB1';")
  expect_equal(row$search_keywords, "")

  # Schema version stamped to 3
  v <- DBI::dbGetQuery(handle$con,
    "SELECT value FROM nxt_meta WHERE key = 'schema_version';")$value
  expect_equal(v, "3")
})
