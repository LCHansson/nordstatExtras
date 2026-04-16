test_that("nxt_open creates a fresh DB with the expected tables", {
  path <- tempfile(fileext = ".sqlite")
  on.exit(unlink(c(path, paste0(path, c("-wal", "-shm"))), force = TRUE))

  handle <- nxt_open(path)
  on.exit(nxt_close(handle), add = TRUE)

  tables <- DBI::dbListTables(handle$con)
  expect_true(all(c("cells", "cell_dims", "queries", "query_cells",
                    "meta_search", "recorded_searches", "nxt_meta") %in% tables))

  ver <- DBI::dbGetQuery(handle$con,
    "SELECT value FROM nxt_meta WHERE key = 'schema_version';")$value
  expect_equal(ver, as.character(nordstatExtras:::NXT_SCHEMA_VERSION))

  # queries has kind + payload columns
  cols <- DBI::dbGetQuery(handle$con, "PRAGMA table_info(queries);")$name
  expect_true(all(c("kind", "payload") %in% cols))
})

test_that("nxt_open is idempotent on existing DB", {
  path <- tempfile(fileext = ".sqlite")
  on.exit(unlink(c(path, paste0(path, c("-wal", "-shm"))), force = TRUE))

  h1 <- nxt_open(path); nxt_close(h1)
  h2 <- nxt_open(path); on.exit(nxt_close(h2), add = TRUE)

  tables <- DBI::dbListTables(h2$con)
  expect_true("cells" %in% tables)
})

test_that("nxt_is_backend detects .sqlite paths and handles", {
  expect_true(nxt_is_backend("foo.sqlite"))
  expect_true(nxt_is_backend("foo.SQLITE3"))
  expect_true(nxt_is_backend("foo.db"))
  expect_false(nxt_is_backend("foo.rds"))
  expect_false(nxt_is_backend(tempdir()))

  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); try(file.remove(path), silent = TRUE) })
  expect_true(nxt_is_backend(handle))
})
