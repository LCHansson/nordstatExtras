# Standalone smoke test: simulates how each of rKolada/rTrafa/pixieweb's
# get_*data functions exercise the nordstatExtras hook path, without hitting
# any live API. Run with: Rscript tests/integration_smoke.R

suppressMessages({
  library(nordstatExtras)
  library(tibble)
})

path <- tempfile(fileext = ".sqlite")
on.exit(unlink(c(path, paste0(path, c("-wal", "-shm"))), force = TRUE))

cat("=== integration smoke test ===\n")
cat("cache file:", path, "\n\n")

handle <- nxt_open(path)

# ---- rKolada simulation -----------------------------------------------------
cat("[rKolada] simulating get_values() with cache=TRUE\n")
kolada_df <- tibble(
  kpi = c("N03700", "N03700"),
  municipality_id = c("0180", "1480"),
  municipality = c("Stockholm", "Göteborg"),
  municipality_type = c("K", "K"),
  year = c(2024L, 2024L),
  value = c(103.2, 90.1)
)

ch <- nxt_cache_handler(
  source = "kolada", entity = "values", cache = TRUE,
  cache_location = handle,
  key_params = list(kpi = "N03700", municipality = c("0180", "1480"),
                    period = 2024L, simplify = TRUE),
  normalize_extra = list(lang = "sv")
)
stopifnot(isFALSE(ch("discover")))
ch("store", kolada_df)
stopifnot(isTRUE(ch("discover")))
back <- ch("load")
stopifnot(nrow(back) == nrow(kolada_df))
stopifnot(all(sort(back$value) == sort(kolada_df$value)))
cat("  OK — stored, discovered, loaded\n")

# ---- rTrafa simulation ------------------------------------------------------
cat("[rTrafa] simulating get_data('t10011', 'itrfslut') with cache=TRUE\n")
trafa_df <- tibble(
  ar = c("2024", "2024"),
  ar_label = c("2024", "2024"),
  drivm = c("102", "103"),
  drivm_label = c("Diesel", "Bensin"),
  itrfslut = c(10500, 9100)
)

ch2 <- nxt_cache_handler(
  source = "trafa", entity = "data", cache = TRUE,
  cache_location = handle,
  key_params = list(product = "t10011", measure = "itrfslut",
                    lang = "SV", simplify = TRUE,
                    ar = "2024", drivm = c("102", "103")),
  normalize_extra = list(product = "t10011", measures = "itrfslut", lang = "SV")
)
stopifnot(isFALSE(ch2("discover")))
ch2("store", trafa_df)
stopifnot(isTRUE(ch2("discover")))
back2 <- ch2("load")
stopifnot(nrow(back2) == nrow(trafa_df))
cat("  OK — stored, discovered, loaded\n")

# ---- pixieweb simulation ----------------------------------------------------
cat("[pixieweb] simulating get_data(scb, 'BE0101N1') with cache=TRUE\n")
px_df <- tibble(
  table_id = rep("BE0101N1", 2),
  Region = c("0180", "1480"),
  Region_text = c("Stockholm", "Göteborg"),
  Tid = c("2024", "2024"),
  Tid_text = c("2024", "2024"),
  value = c(984000, 585000)
)

ch3 <- nxt_cache_handler(
  source = "pixieweb", entity = "data", cache = TRUE,
  cache_location = handle,
  key_params = list(alias = "scb", table_id = "BE0101N1",
                    .output = "long", simplify = TRUE,
                    Region = c("0180", "1480"), Tid = "2024"),
  normalize_extra = list(alias = "scb")
)
stopifnot(isFALSE(ch3("discover")))
ch3("store", px_df)
stopifnot(isTRUE(ch3("discover")))
back3 <- ch3("load")
stopifnot(nrow(back3) == nrow(px_df))
cat("  OK — stored, discovered, loaded\n")

# ---- Cross-source coexistence ----------------------------------------------
n_cells <- DBI::dbGetQuery(handle$con, "SELECT COUNT(*) AS n FROM cells")$n
n_queries <- DBI::dbGetQuery(handle$con, "SELECT COUNT(*) AS n FROM queries")$n
by_source <- DBI::dbGetQuery(handle$con,
  "SELECT source, COUNT(*) AS n FROM cells GROUP BY source")
cat("\n[db inventory] total cells:", n_cells,
    "| total queries:", n_queries, "\n")
print(by_source)

# ---- nxt_is_backend detection -----------------------------------------------
stopifnot(nxt_is_backend(path))
stopifnot(nxt_is_backend(handle))
stopifnot(!nxt_is_backend(tempdir()))
cat("\n[nxt_is_backend] OK on path, handle, and rejection of non-sqlite\n")

# ---- Metadata roundtrips (kind = 'metadata') --------------------------------
cat("\n=== metadata path ===\n")

# rKolada: flat tibble with id + title + description
cat("[kolada/kpi] flat tibble with Swedish titles\n")
kpi_df <- tibble(
  id = c("N03700", "N01951"),
  title = c("Befolkning totalt", "Bruttoregionprodukt per capita"),
  description = c("Antal invånare", "BRP per capita")
)
ch_kpi <- nxt_cache_handler(
  source = "kolada", entity = "kpi", cache = TRUE,
  cache_location = handle, kind = "metadata",
  key_params = list(id = NULL)
)
ch_kpi("store", kpi_df)
stopifnot(isTRUE(ch_kpi("discover")))
back_kpi <- ch_kpi("load")
stopifnot(identical(back_kpi, kpi_df))
cat("  OK — roundtrip identical\n")

# rKolada: nested list column (kpi_groups.members)
cat("[kolada/kpi_groups] nested list column\n")
kpi_groups_df <- tibble(
  id = c("G01", "G02"),
  title = c("Befolkning", "Ekonomi"),
  members = list(
    tibble(member_id = c("N03700", "N03701"),
           member_title = c("A", "B")),
    tibble(member_id = "N01951", member_title = "BRP")
  )
)
ch_grp <- nxt_cache_handler(
  source = "kolada", entity = "kpi_groups", cache = TRUE,
  cache_location = handle, kind = "metadata",
  key_params = list()
)
ch_grp("store", kpi_groups_df)
back_grp <- ch_grp("load")
stopifnot(identical(back_grp, kpi_groups_df))
stopifnot(tibble::is_tibble(back_grp$members[[1]]))
cat("  OK — nested list column byte-identical\n")

# rTrafa: opaque list (not a tibble)
cat("[trafa/structure] opaque list roundtrip\n")
structure_list <- list(
  product = "t10011",
  items = list(
    list(type = "D", name = "ar", values = list("2023", "2024")),
    list(type = "M", name = "itrfslut")
  )
)
ch_str <- nxt_cache_handler(
  source = "trafa", entity = "structure", cache = TRUE,
  cache_location = handle, kind = "metadata",
  key_params = list(product = "t10011", lang = "SV")
)
ch_str("store", structure_list)
back_str <- ch_str("load")
stopifnot(identical(back_str, structure_list))
cat("  OK — list roundtrip identical\n")

# pixieweb: tibble with px_api attribute
cat("[pixieweb/enriched] tibble with px_api attribute\n")
enriched_df <- tibble(
  id = c("BE0101N1", "AM0401A"),
  title = c("Folkmängd efter region", "Arbetskraftsundersökningen"),
  description = c("Kommunal befolkningsstatistik", "AKU"),
  contents = c("Folkmängd", "Sysselsättning")
)
fake_api <- structure(list(alias = "scb", lang = "sv", version = "v2"),
                      class = "px_api")
attr(enriched_df, "px_api") <- fake_api

ch_enr <- nxt_cache_handler(
  source = "pixieweb", entity = "enriched", cache = TRUE,
  cache_location = handle, kind = "metadata",
  key_params = list(alias = "scb", lang = "sv", ids = "a,b")
)
ch_enr("store", enriched_df)
back_enr <- ch_enr("load")
stopifnot(identical(attr(back_enr, "px_api"), fake_api))
stopifnot(inherits(attr(back_enr, "px_api"), "px_api"))
cat("  OK — px_api attribute preserved\n")

# ---- Typeahead search ------------------------------------------------------
cat("\n=== typeahead search ===\n")

hits <- nxt_search(handle, "bef*")
cat("[nxt_search('bef*')] returned", nrow(hits), "hits\n")
print(hits[, c("source", "entity_type", "entity_id", "title")])
stopifnot(nrow(hits) > 0)
stopifnot("kolada" %in% hits$source)

# Source filter
only_px <- nxt_search(handle, "folk*", sources = "pixieweb")
cat("[nxt_search('folk*', sources='pixieweb')] returned",
    nrow(only_px), "hit(s)\n")
stopifnot(all(only_px$source == "pixieweb"))

# Diacritics-insensitive: 'ar' should match 'år' tokenized via unicode61
hits_ar <- nxt_search(handle, "brutto*")
cat("[nxt_search('brutto*')] returned", nrow(hits_ar), "hit(s)\n")
stopifnot(nrow(hits_ar) >= 1)

# No matches
stopifnot(nrow(nxt_search(handle, "zzxyqwertyu*")) == 0)

# ---- Mixed-kind coexistence (data + metadata in the same file) -------------
kinds <- DBI::dbGetQuery(handle$con,
  "SELECT kind, COUNT(*) AS n FROM queries GROUP BY kind ORDER BY kind;")
cat("\n[db inventory] queries by kind:\n")
print(kinds)
stopifnot(all(c("data", "metadata") %in% kinds$kind))

nxt_close(handle)
cat("\n=== ALL SMOKE TESTS PASSED ===\n")
