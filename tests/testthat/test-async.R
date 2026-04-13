test_that("nxt_write_async falls back to sync when mirai missing", {
  # We can't uninstall mirai inside the test; instead, if it IS installed
  # we just verify the write ends up persisted.
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); try(file.remove(path), silent = TRUE) })

  df <- fixture_kolada()
  suppressWarnings({
    nxt_write_async(handle, "kolada", "values",
                    key_params = list(q = 1), df = df)
    nxt_flush(handle)
  })

  n <- DBI::dbGetQuery(handle$con,
                       "SELECT COUNT(*) AS n FROM queries;")$n
  expect_gte(n, 1L)
})
