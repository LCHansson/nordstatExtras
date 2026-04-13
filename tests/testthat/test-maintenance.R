test_that("nxt_clear removes matching queries and orphan cells", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); try(file.remove(path), silent = TRUE) })

  a <- nxt_cache_handler("kolada", "values", TRUE, handle,
                         key_params = list(x = 1))
  b <- nxt_cache_handler("kolada", "values", TRUE, handle,
                         key_params = list(x = 2))
  a("store", fixture_kolada())
  b("store", fixture_kolada())

  suppressMessages(nxt_clear(handle, source = "kolada"))

  n <- DBI::dbGetQuery(handle$con, "SELECT COUNT(*) AS n FROM queries;")$n
  n_cells <- DBI::dbGetQuery(handle$con, "SELECT COUNT(*) AS n FROM cells;")$n
  expect_equal(n, 0L)
  expect_equal(n_cells, 0L)
})

test_that("nxt_gc removes stale cells and their orphaned queries", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); try(file.remove(path), silent = TRUE) })

  ch <- nxt_cache_handler("kolada", "values", TRUE, handle,
                          key_params = list(x = 1))
  ch("store", fixture_kolada())

  DBI::dbExecute(handle$con,
    "UPDATE cells SET fetched_at = fetched_at - (60 * 86400);")

  suppressMessages(nxt_gc(handle, max_age_days = 30))

  n_cells <- DBI::dbGetQuery(handle$con,
                             "SELECT COUNT(*) AS n FROM cells;")$n
  n_queries <- DBI::dbGetQuery(handle$con,
                               "SELECT COUNT(*) AS n FROM queries;")$n
  expect_equal(n_cells, 0L)
  expect_equal(n_queries, 0L)
})

test_that("nxt_gc keeps queries whose cells are still fresh", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); try(file.remove(path), silent = TRUE) })

  ch <- nxt_cache_handler("kolada", "values", TRUE, handle,
                          key_params = list(x = 1))
  ch("store", fixture_kolada())

  suppressMessages(nxt_gc(handle, max_age_days = 30))

  expect_equal(
    DBI::dbGetQuery(handle$con, "SELECT COUNT(*) AS n FROM queries;")$n,
    1L
  )
  expect_true(ch("discover"))
})
