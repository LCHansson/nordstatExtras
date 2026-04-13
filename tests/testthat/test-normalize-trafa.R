test_that("normalize_trafa + reconstruct_trafa roundtrip", {
  df <- fixture_trafa()
  norm <- normalize_trafa(df, product = "t10011", measures = "itrfslut")
  back <- reconstruct_trafa(norm$cells, norm$template)

  expect_equal(nrow(back), nrow(df))
  expect_true("itrfslut" %in% names(back))

  key <- function(x) x[order(x$ar, x$drivm), ]
  expect_equal(key(back)$itrfslut, key(df)$itrfslut)
  expect_equal(key(back)$drivm_label, key(df)$drivm_label)
})

test_that("normalize_trafa auto-detects measures", {
  df <- fixture_trafa()
  norm <- normalize_trafa(df, product = "t10011")
  expect_true("itrfslut" %in% norm$template$measures)
})

test_that("normalize_trafa uses ar column as period", {
  df <- fixture_trafa()
  norm <- normalize_trafa(df, product = "t10011", measures = "itrfslut")
  expect_equal(sort(unique(norm$cells$period)), c("2023", "2024"))
})
