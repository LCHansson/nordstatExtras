suppressMessages(devtools::load_all("nordstatExtras", quiet = TRUE))

n_rows <- 290L * 192L
df <- tibble::tibble(
  table_id = rep("TAB_PERF", n_rows),
  Region = rep(sprintf("%04d", 1:290), each = 192),
  Region_text = rep(sprintf("Kommun %d", 1:290), each = 192),
  Tid = rep(sprintf("%dM%02d", rep(2010:2025, each = 12), rep(1:12, 16)), times = 290),
  Tid_text = rep(sprintf("%dM%02d", rep(2010:2025, each = 12), rep(1:12, 16)), times = 290),
  value = runif(n_rows, 0, 1000)
)
cat("n_rows =", nrow(df), "\n")

path <- tempfile(fileext = ".sqlite")
h <- nxt_open(path)
on.exit({
  nxt_close(h)
  unlink(c(path, paste0(path, c("-wal", "-shm"))), force = TRUE)
})

ch <- nxt_cache_handler(
  "pixieweb", "data", TRUE, h,
  key_params = list(alias = "scb", table_id = "TAB_PERF", Tid = "all"),
  normalize_extra = list(alias = "scb")
)
cat("store:    ", sprintf("%6.2f s", system.time(ch("store", df))["elapsed"]), "\n")
cat("discover: ", sprintf("%6.2f s", system.time(res <- ch("discover"))["elapsed"]),
    "(", res, ")\n")
cat("load:     ", sprintf("%6.2f s", system.time(back <- ch("load"))["elapsed"]),
    "(", nrow(back), "rows)\n")
