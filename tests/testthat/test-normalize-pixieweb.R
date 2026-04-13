test_that("normalize_pixieweb + reconstruct_pixieweb roundtrip", {
  df <- fixture_pixieweb()
  norm <- normalize_pixieweb(df, alias = "scb")
  back <- reconstruct_pixieweb(norm$cells, norm$template)

  expect_equal(nrow(back), nrow(df))
  expect_true(all(c("table_id", "Region", "Tid", "value") %in% names(back)))

  key <- function(x) x[order(x$Region, x$Tid), ]
  expect_equal(key(back)$value, key(df)$value)
  expect_equal(key(back)$Region_text, key(df)$Region_text)
})

test_that("normalize_pixieweb errors without required columns", {
  bad <- tibble::tibble(Region = "X", value = 1)
  expect_error(normalize_pixieweb(bad), "table_id")
})

test_that("normalize_pixieweb detects Tid as period", {
  df <- fixture_pixieweb()
  norm <- normalize_pixieweb(df, alias = "scb")
  expect_equal(sort(unique(norm$cells$period)), c("2023", "2024"))
})
