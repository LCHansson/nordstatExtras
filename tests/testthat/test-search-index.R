test_that("nxt_search returns hits across sources for a Swedish prefix", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE) })

  # Populate search index via metadata stores for all three sources
  ch_kpi <- nxt_cache_handler("kolada", "kpi", TRUE, handle, kind = "metadata",
                              key_params = list())
  ch_prd <- nxt_cache_handler("trafa", "products", TRUE, handle, kind = "metadata",
                              key_params = list(lang = "SV"))
  ch_tab <- nxt_cache_handler("pixieweb", "tables", TRUE, handle, kind = "metadata",
                              key_params = list(alias = "scb"))

  ch_kpi("store", fixture_kpi())           # includes "Befolkning totalt"
  ch_prd("store", fixture_products())      # includes "Bussar i trafik"
  ch_tab("store", fixture_tables())        # includes "Folkmängd ..."

  # Prefix match across sources
  hits <- nxt_search(handle, "bef*")
  expect_gt(nrow(hits), 0)
  expect_true("kolada" %in% hits$source)
})

test_that("nxt_search source filter restricts results", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE) })

  nxt_cache_handler("kolada", "kpi", TRUE, handle, kind = "metadata",
                    key_params = list())("store", fixture_kpi())
  nxt_cache_handler("pixieweb", "tables", TRUE, handle, kind = "metadata",
                    key_params = list(alias = "scb"))("store", fixture_tables())

  only_kolada <- nxt_search(handle, "bef*", sources = "kolada")
  expect_true(all(only_kolada$source == "kolada"))

  only_px <- nxt_search(handle, "bef*", sources = "pixieweb")
  expect_true(all(only_px$source == "pixieweb"))
})

test_that("nxt_search with diacritics — 'ar' matches 'År' via unicode61", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE) })

  df <- tibble::tibble(
    id = "TEST1",
    title = "År och kön",
    description = "Årsstatistik per kön"
  )
  nxt_cache_handler("kolada", "kpi", TRUE, handle, kind = "metadata",
                    key_params = list())("store", df)

  hits <- nxt_search(handle, "ar*")
  expect_gt(nrow(hits), 0)
})

test_that("non-searchable entities are not indexed", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE) })

  # municipality has no extractor registered — storing it must NOT add rows
  # to meta_search.
  nxt_cache_handler("kolada", "municipality", TRUE, handle, kind = "metadata",
                    key_params = list())("store", fixture_municipality())

  n <- DBI::dbGetQuery(handle$con,
                       "SELECT COUNT(*) AS n FROM meta_search;")$n
  expect_equal(n, 0L)
})

test_that("nxt_search returns empty tibble for a non-matching query", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE) })

  nxt_cache_handler("kolada", "kpi", TRUE, handle, kind = "metadata",
                    key_params = list())("store", fixture_kpi())

  hits <- nxt_search(handle, "zxyqwerty*")
  expect_equal(nrow(hits), 0L)
  expect_named(hits, c("source", "entity_type", "entity_id",
                       "title", "description", "rank"))
})

test_that("search index updates on re-store of the same query", {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({ nxt_close(handle); unlink(c(path, paste0(path, c("-wal","-shm"))), force = TRUE) })

  ch <- nxt_cache_handler("kolada", "kpi", TRUE, handle, kind = "metadata",
                          key_params = list())

  first <- fixture_kpi()
  ch("store", first)

  # Update: change title of the first KPI
  second <- first
  second$title[1] <- "Totalt antal invånare"
  ch("store", second)

  hits <- nxt_search(handle, "totalt*")
  expect_true(any(grepl("Totalt", hits$title)))
})
