# Synthetic fixtures matching the metadata column shapes returned by
# rKolada / rTrafa / pixieweb. Offline — no live API calls. Used by the
# metadata caching and search tests.

fixture_kpi <- function() {
  tibble::tibble(
    id = c("N03700", "N03701", "N01951"),
    title = c("Befolkning totalt", "Invånare per åldersgrupp",
              "Bruttoregionprodukt per capita"),
    description = c("Antal invånare per 31/12",
                    "Invånare uppdelat på åldersgrupper",
                    "BRP per capita i löpande priser"),
    auspice = c("SCB", "SCB", "SCB"),
    has_ou_data = c(FALSE, FALSE, TRUE),
    is_divided_by_gender = c(TRUE, TRUE, FALSE)
  )
}

fixture_kpi_groups <- function() {
  # kpi_groups has a nested list column `members` of inner tibbles.
  members_one <- tibble::tibble(
    member_id = c("N03700", "N03701"),
    member_title = c("Befolkning totalt", "Invånare per åldersgrupp")
  )
  members_two <- tibble::tibble(
    member_id = c("N01951", "N01952"),
    member_title = c("BRP per capita", "Sysselsättningsgrad")
  )
  tibble::tibble(
    id = c("G01", "G02"),
    title = c("Befolkningsindikatorer", "Ekonomiska nyckeltal"),
    members = list(members_one, members_two)
  )
}

fixture_municipality <- function() {
  tibble::tibble(
    id = c("0180", "1480", "1280"),
    title = c("Stockholm", "Göteborg", "Malmö"),
    type = c("K", "K", "K")
  )
}

fixture_products <- function() {
  tibble::tibble(
    name = c("t10011", "t10026", "t10016"),
    label = c("Bussar i trafik", "Lastbilar", "Personbilar"),
    description = c("Antal bussar i trafik per drivmedel",
                    "Lastbilsstatistik",
                    "Personbilsstatistik"),
    id = c(1L, 2L, 3L),
    active_from = c("2000-01-01", "2000-01-01", "2000-01-01")
  )
}

fixture_structure_list <- function() {
  # rTrafa get_structure_raw() returns a list of nested StructureItems,
  # not a tibble. Mimic the shape loosely — the point is that it's a list.
  list(
    product = "t10011",
    items = list(
      list(type = "D", name = "ar", label = "År",
           values = list(list(name = "2023", label = "2023"),
                         list(name = "2024", label = "2024"))),
      list(type = "D", name = "drivm", label = "Drivmedel",
           values = list(list(name = "102", label = "Diesel"),
                         list(name = "103", label = "Bensin"))),
      list(type = "M", name = "itrfslut", label = "Antal i trafik")
    )
  )
}

fixture_tables <- function() {
  tibble::tibble(
    id = c("BE0101N1", "BE0101N2", "AM0401A"),
    title = c("Folkmängd efter region, civilstånd, ålder och kön",
              "Befolkningsförändringar",
              "Arbetskraftsundersökningen"),
    description = c("Folkmängdsstatistik per kommun och år",
                    "Födelse, död, migration",
                    "Sysselsättning och arbetslöshet"),
    category = c("population", "population", "labour"),
    updated = c("2025-02-01", "2025-02-01", "2025-01-15"),
    first_period = c("1968", "1968", "2005"),
    last_period = c("2024", "2024", "2024"),
    time_unit = c("year", "year", "month"),
    variables = list(
      c("Region", "Civilstand", "Alder", "Kon", "Tid"),
      c("Region", "Handelse", "Tid"),
      c("Region", "Kon", "Alder", "Tid")
    ),
    subject_code = c("BE", "BE", "AM"),
    subject_path = c("BE/BE0101", "BE/BE0101", "AM/AM0401"),
    source = c("SCB", "SCB", "SCB"),
    discontinued = c(FALSE, FALSE, FALSE)
  )
}

# An enriched tibble (output of table_enrich()) — same columns as fixture_tables
# plus the enrichment columns, and a px_api attribute.
fixture_tables_enriched <- function() {
  df <- fixture_tables()
  df$notes <- list(
    c("Data reviderad 2025-01-01", "Källa: SCB"),
    character(),
    c("Kvartalsvis uppdatering")
  )
  df$contents <- c("Folkmängd", "Födelser och dödsfall", "Sysselsättning")
  df$subject_area <- c("Befolkning", "Befolkning", "Arbetsmarknad")
  df$official_statistics <- c(TRUE, TRUE, TRUE)
  df$contact <- c("SCB, Befolkningsstatistik", "SCB, Befolkningsstatistik",
                  "SCB, Arbetsmarknadsstatistik")
  # Fake px_api attribute — the real table_enrich attaches a px_api S3 object.
  fake_api <- structure(
    list(alias = "scb", lang = "sv", version = "v2"),
    class = "px_api"
  )
  attr(df, "px_api") <- fake_api
  df
}
