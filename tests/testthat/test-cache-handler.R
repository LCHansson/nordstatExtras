test_that("cache handler store/discover/load roundtrips a kolada df", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); try(file.remove(path), silent = TRUE) })

  df <- fixture_kolada()
  ch <- nxt_cache_handler(
    source = "kolada", entity = "values", cache = TRUE,
    cache_location = handle,
    key_params = list(kpi = "N03700", years = "2023-2024")
  )

  expect_false(ch("discover"))
  ch("store", df)
  expect_true(ch("discover"))

  back <- ch("load")
  expect_equal(nrow(back), nrow(df))
  expect_setequal(names(back), names(df))
})

test_that("cache handler returns a no-op when cache = FALSE", {
  ch <- nxt_cache_handler(
    source = "kolada", entity = "values", cache = FALSE,
    cache_location = tempfile(fileext = ".sqlite")
  )
  expect_false(ch("discover"))
  expect_identical(ch("store", fixture_kolada()), fixture_kolada())
})

test_that("different key_params produce different cache entries", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); try(file.remove(path), silent = TRUE) })

  mk <- function(params) {
    nxt_cache_handler("kolada", "values", TRUE, handle, key_params = params)
  }

  a <- mk(list(kpi = "X")); a("store", fixture_kolada())
  b <- mk(list(kpi = "Y"))
  expect_false(b("discover"))
  expect_true(a("discover"))
})

test_that("cells are deduplicated across overlapping queries", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); try(file.remove(path), silent = TRUE) })

  df <- fixture_kolada()

  # Two queries that would return the same underlying cells
  h1 <- nxt_cache_handler("kolada", "values", TRUE, handle,
                          key_params = list(q = "a"))
  h2 <- nxt_cache_handler("kolada", "values", TRUE, handle,
                          key_params = list(q = "b"))

  h1("store", df)
  h2("store", df)

  n_cells <- DBI::dbGetQuery(handle$con, "SELECT COUNT(*) AS n FROM cells;")$n
  n_qcells <- DBI::dbGetQuery(handle$con,
                              "SELECT COUNT(*) AS n FROM query_cells;")$n
  # Dedup: cells count equals nrow(df), but query_cells maps both queries
  expect_equal(n_cells, nrow(df))
  expect_equal(n_qcells, 2 * nrow(df))
})

test_that("TTL is cell-level: ignores queries.fetched_at, honors cells.fetched_at", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); try(file.remove(path), silent = TRUE) })

  ch <- nxt_cache_handler("kolada", "values", TRUE, handle,
                          key_params = list(x = 1), ttl_days = 30)
  ch("store", fixture_kolada())
  expect_true(ch("discover"))

  # Backdating ONLY the queries row must not expire anything —
  # cells are still fresh.
  DBI::dbExecute(handle$con,
    "UPDATE queries SET fetched_at = fetched_at - (60 * 86400);")
  expect_true(ch("discover"))

  # Backdating cells themselves must expire the query.
  DBI::dbExecute(handle$con,
    "UPDATE cells SET fetched_at = fetched_at - (60 * 86400);")
  expect_false(ch("discover"))
  expect_null(ch("load"))
})

test_that("cross-query freshness: refresh on one query propagates to another", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); try(file.remove(path), silent = TRUE) })

  df <- fixture_kolada()

  h1 <- nxt_cache_handler("kolada", "values", TRUE, handle,
                          key_params = list(q = "a"), ttl_days = 30)
  h1("store", df)

  # Age h1's cells to just past the TTL — it would go stale on its own.
  DBI::dbExecute(handle$con,
    "UPDATE cells SET fetched_at = fetched_at - (40 * 86400);")
  expect_false(h1("discover"))

  # A different query stores the same underlying cells → cells get
  # refreshed via UPSERT. h1 should now see them as fresh even though
  # its own queries row was never touched by the refresh.
  h2 <- nxt_cache_handler("kolada", "values", TRUE, handle,
                          key_params = list(q = "b"), ttl_days = 30)
  h2("store", df)

  expect_true(h1("discover"))
  back <- h1("load")
  expect_equal(nrow(back), nrow(df))
})

test_that("discover fails when n_present < n_expected (cells GC'd away)", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); try(file.remove(path), silent = TRUE) })

  ch <- nxt_cache_handler("kolada", "values", TRUE, handle,
                          key_params = list(x = 1))
  ch("store", fixture_kolada())
  expect_true(ch("discover"))

  # Drop one junction row — n_present becomes < n_expected
  DBI::dbExecute(handle$con,
    "DELETE FROM query_cells
       WHERE rowid = (SELECT rowid FROM query_cells LIMIT 1);")
  expect_false(ch("discover"))
})
