# Synthetic fixtures that mimic the column shapes returned by the three
# source packages. No live API calls — safe for CRAN.

fixture_kolada <- function() {
  tibble::tibble(
    kpi = c("N03700", "N03700", "N03700", "N03700"),
    municipality_id = c("0180", "0180", "1480", "1480"),
    municipality = c("Stockholm", "Stockholm", "Göteborg", "Göteborg"),
    municipality_type = c("K", "K", "K", "K"),
    year = c(2023L, 2024L, 2023L, 2024L),
    value = c(101.5, 103.2, 88.4, 90.1),
    gender = c("T", "T", "T", "T")
  )
}

fixture_trafa <- function() {
  tibble::tibble(
    ar = c("2023", "2023", "2024", "2024"),
    ar_label = c("2023", "2023", "2024", "2024"),
    drivm = c("102", "103", "102", "103"),
    drivm_label = c("Diesel", "Bensin", "Diesel", "Bensin"),
    itrfslut = c(10086, 9420, 10500, 9100)
  )
}

fixture_pixieweb <- function() {
  tibble::tibble(
    table_id = rep("BE0101N1", 4),
    Region = c("0180", "0180", "1480", "1480"),
    Region_text = c("Stockholm", "Stockholm", "Göteborg", "Göteborg"),
    Tid = c("2023", "2024", "2023", "2024"),
    Tid_text = c("2023", "2024", "2023", "2024"),
    value = c(975000, 984000, 580000, 585000)
  )
}

with_temp_cache <- function(code) {
  path <- tempfile(fileext = ".sqlite")
  handle <- nxt_open(path)
  on.exit({
    try(nxt_close(handle), silent = TRUE)
    try(file.remove(path), silent = TRUE)
  })
  force(code)
}
