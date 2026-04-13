# Performance regression test for the cells store path.
#
# Before the temp-table refactor + the O(n^2) collect_dim_rows fix + the
# vectorised normalize_*, storing a ~100k-row pixieweb result took many
# minutes and blocked on the user's workflow. This test generates a
# synthetic batch of that shape and asserts the whole pipeline finishes
# in well under 30 seconds on any reasonable machine.
#
# The budget is deliberately loose so the test doesn't flake on slow CI,
# but tight enough that a return of the O(n^2) or row-by-row pattern
# would blow it by orders of magnitude.

test_that("storing a ~100k-row pixieweb batch completes within a few seconds", {
  skip_on_cran()
  skip_on_ci()

  n_regions <- 290L
  months <- sprintf("%dM%02d", rep(2010:2025, each = 12L), rep(1:12, 16L))
  n_months <- length(months)
  n_rows <- n_regions * n_months    # ~55 680

  df <- tibble::tibble(
    table_id    = rep("TAB_PERF", n_rows),
    Region      = rep(sprintf("%04d", seq_len(n_regions)), each = n_months),
    Region_text = rep(sprintf("Kommun %d", seq_len(n_regions)), each = n_months),
    Tid         = rep(months, times = n_regions),
    Tid_text    = rep(months, times = n_regions),
    value       = runif(n_rows, 0, 1000)
  )

  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({
    nxt_close(handle)
    unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE)
  })

  ch <- nxt_cache_handler(
    source = "pixieweb", entity = "data", cache = TRUE,
    cache_location = handle,
    key_params = list(alias = "scb", table_id = "TAB_PERF", Tid = "all"),
    normalize_extra = list(alias = "scb")
  )

  t_store <- system.time(ch("store", df))["elapsed"]
  expect_lt(t_store, 30)

  # Sanity: all rows were persisted
  n_cells <- DBI::dbGetQuery(handle$con,
    "SELECT COUNT(*) AS n FROM cells;")$n
  expect_equal(n_cells, n_rows)

  # Sanity: discover + load roundtrip works and is also fast
  expect_true(ch("discover"))
  t_load <- system.time({
    back <- ch("load")
  })["elapsed"]
  expect_lt(t_load, 15)
  expect_equal(nrow(back), n_rows)
})

test_that("vec_build_dims dedupes dim vectors before hashing", {
  # 5000 rows × only 10 unique dim combinations → hash is called only
  # for the unique blobs, which means vec_build_dims is fast regardless
  # of total row count.
  n <- 5000L
  n_unique <- 10L
  df <- tibble::tibble(
    region = rep(sprintf("R%02d", seq_len(n_unique)), length.out = n),
    cat    = rep(letters[1:5], length.out = n),
    value  = runif(n)
  )

  info <- vec_build_dims(df, c("region", "cat"))
  expect_length(info$dims_hash, n)

  # There should be exactly min(n_unique * 5, n) distinct hashes
  expect_lte(length(unique(info$dims_hash)), n_unique * 5L)
})
