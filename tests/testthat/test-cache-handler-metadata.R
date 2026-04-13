test_that("metadata cache roundtrips a flat tibble (kolada/kpi)", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE) })

  df <- fixture_kpi()
  ch <- nxt_cache_handler(
    source = "kolada", entity = "kpi", cache = TRUE,
    cache_location = handle,
    kind = "metadata",
    key_params = list(id = NULL)
  )

  expect_false(ch("discover"))
  ch("store", df)
  expect_true(ch("discover"))

  back <- ch("load")
  expect_equal(back, df)
})

test_that("metadata cache preserves nested list columns (kpi_groups)", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE) })

  df <- fixture_kpi_groups()
  ch <- nxt_cache_handler(
    source = "kolada", entity = "kpi_groups", cache = TRUE,
    cache_location = handle,
    kind = "metadata",
    key_params = list()
  )
  ch("store", df)
  back <- ch("load")

  # The `members` list column must survive byte-identically
  expect_identical(back, df)
  expect_true(tibble::is_tibble(back$members[[1]]))
  expect_equal(back$members[[1]]$member_id, df$members[[1]]$member_id)
})

test_that("metadata cache stores and loads a list (rTrafa structure)", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE) })

  payload <- fixture_structure_list()
  ch <- nxt_cache_handler(
    source = "trafa", entity = "structure", cache = TRUE,
    cache_location = handle,
    kind = "metadata",
    key_params = list(product = "t10011", lang = "SV")
  )
  ch("store", payload)
  back <- ch("load")

  # List (not tibble) must roundtrip identically
  expect_identical(back, payload)
  expect_type(back, "list")
  expect_equal(back$product, "t10011")
})

test_that("metadata cache preserves R attributes like px_api", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE) })

  df <- fixture_tables_enriched()
  ch <- nxt_cache_handler(
    source = "pixieweb", entity = "enriched", cache = TRUE,
    cache_location = handle,
    kind = "metadata",
    key_params = list(alias = "scb", lang = "sv", ids = "a,b,c")
  )
  ch("store", df)
  back <- ch("load")

  expect_equal(back, df)
  expect_identical(attr(back, "px_api"), attr(df, "px_api"))
  expect_s3_class(attr(back, "px_api"), "px_api")
})

test_that("metadata TTL is query-level (queries.fetched_at)", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE) })

  ch <- nxt_cache_handler(
    source = "kolada", entity = "kpi", cache = TRUE,
    cache_location = handle,
    kind = "metadata",
    key_params = list(id = "X"),
    ttl_days = 30
  )
  ch("store", fixture_kpi())
  expect_true(ch("discover"))

  DBI::dbExecute(handle$con,
    "UPDATE queries SET fetched_at = fetched_at - (60 * 86400)
       WHERE kind = 'metadata';")

  expect_false(ch("discover"))
  expect_null(ch("load"))
})

test_that("kind mismatch returns NULL (data handler won't see metadata row)", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE) })

  # Store metadata under a specific key
  meta_ch <- nxt_cache_handler(
    source = "kolada", entity = "kpi", cache = TRUE,
    cache_location = handle,
    kind = "metadata",
    key_params = list(x = 1)
  )
  meta_ch("store", fixture_kpi())

  # A data handler with the SAME source/entity/key_params would hash to
  # the same query_hash — but expected_kind should make it return NULL.
  data_ch <- nxt_cache_handler(
    source = "kolada", entity = "kpi", cache = TRUE,
    cache_location = handle,
    kind = "data",
    key_params = list(x = 1)
  )
  expect_false(data_ch("discover"))
  expect_null(data_ch("load"))

  # The metadata handler still works
  expect_true(meta_ch("discover"))
})
