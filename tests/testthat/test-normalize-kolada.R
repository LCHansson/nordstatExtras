test_that("normalize_kolada roundtrips via reconstruct_kolada", {
  df <- fixture_kolada()
  norm <- normalize_kolada(df)
  back <- reconstruct_kolada(norm$cells, norm$template)

  expect_equal(nrow(back), nrow(df))
  expect_setequal(names(back), names(df))

  # Sort both by kpi, year, municipality_id for stable comparison
  key <- function(x) x[order(x$kpi, x$year, x$municipality_id), ]
  expect_equal(key(back)$value, key(df)$value)
  expect_equal(key(back)$municipality, key(df)$municipality)
  expect_equal(as.character(key(back)$year), as.character(key(df)$year))
})

test_that("normalize_kolada produces one cell per row", {
  df <- fixture_kolada()
  norm <- normalize_kolada(df)
  expect_equal(nrow(norm$cells), nrow(df))
  expect_true(all(norm$cells$source == "kolada"))
})

test_that("normalize_kolada errors on missing required columns", {
  df <- tibble::tibble(kpi = "X", value = 1)  # no year
  expect_error(normalize_kolada(df), "year")
})
