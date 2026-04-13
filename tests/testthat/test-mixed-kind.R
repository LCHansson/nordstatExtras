test_that("same SQLite file holds both data cells and metadata blobs", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE) })

  # Store data
  data_ch <- nxt_cache_handler(
    "kolada", "values", TRUE, handle,
    key_params = list(kpi = "N03700")
  )
  data_ch("store", fixture_kolada())

  # Store metadata
  meta_ch <- nxt_cache_handler(
    "kolada", "kpi", TRUE, handle,
    kind = "metadata",
    key_params = list(id = "N03700")
  )
  meta_ch("store", fixture_kpi())

  # Both coexist
  expect_true(data_ch("discover"))
  expect_true(meta_ch("discover"))

  # Separate rows in queries, with different kinds
  kinds <- DBI::dbGetQuery(handle$con,
    "SELECT kind, COUNT(*) AS n FROM queries GROUP BY kind ORDER BY kind;")
  expect_equal(nrow(kinds), 2L)
  expect_equal(sort(kinds$kind), c("data", "metadata"))

  # cells is non-empty, meta_search is non-empty
  expect_gt(DBI::dbGetQuery(handle$con,
            "SELECT COUNT(*) AS n FROM cells;")$n, 0L)
  expect_gt(DBI::dbGetQuery(handle$con,
            "SELECT COUNT(*) AS n FROM meta_search;")$n, 0L)
})

test_that("nxt_clear(source='kolada') removes both kinds", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE) })

  nxt_cache_handler("kolada", "values", TRUE, handle,
                    key_params = list(kpi = "X"))("store", fixture_kolada())
  nxt_cache_handler("kolada", "kpi", TRUE, handle, kind = "metadata",
                    key_params = list())("store", fixture_kpi())
  # Also add a non-kolada entry that should survive
  nxt_cache_handler("pixieweb", "tables", TRUE, handle, kind = "metadata",
                    key_params = list(alias = "scb"))("store", fixture_tables())

  suppressMessages(nxt_clear(handle, source = "kolada"))

  # No kolada rows left in queries
  n_kolada <- DBI::dbGetQuery(handle$con,
    "SELECT COUNT(*) AS n FROM queries WHERE source = 'kolada';")$n
  expect_equal(n_kolada, 0L)

  # pixieweb entry survives
  n_px <- DBI::dbGetQuery(handle$con,
    "SELECT COUNT(*) AS n FROM queries WHERE source = 'pixieweb';")$n
  expect_equal(n_px, 1L)

  # cells tied to kolada are gone
  n_cells <- DBI::dbGetQuery(handle$con,
    "SELECT COUNT(*) AS n FROM cells;")$n
  expect_equal(n_cells, 0L)

  # meta_search for kolada is gone, pixieweb survives
  sources <- DBI::dbGetQuery(handle$con,
    "SELECT DISTINCT source FROM meta_search;")$source
  expect_false("kolada" %in% sources)
  expect_true("pixieweb" %in% sources)
})

test_that("nxt_gc cleans metadata and data independently", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE) })

  nxt_cache_handler("kolada", "values", TRUE, handle,
                    key_params = list(kpi = "X"))("store", fixture_kolada())
  nxt_cache_handler("kolada", "kpi", TRUE, handle, kind = "metadata",
                    key_params = list())("store", fixture_kpi())

  # Backdate ALL fetched_at on queries (both cells and metadata rows)
  DBI::dbExecute(handle$con,
    "UPDATE queries SET fetched_at = fetched_at - (60 * 86400);")
  DBI::dbExecute(handle$con,
    "UPDATE cells SET fetched_at = fetched_at - (60 * 86400);")

  suppressMessages(nxt_gc(handle, max_age_days = 30))

  # Both kinds of queries should be gone
  n_q <- DBI::dbGetQuery(handle$con,
                         "SELECT COUNT(*) AS n FROM queries;")$n
  expect_equal(n_q, 0L)
})
