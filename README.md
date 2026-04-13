# nordstatExtras

<!-- badges: start -->
<!-- badges: end -->

Shared SQLite cache backend for the nordstat family of R packages:
[rKolada](https://github.com/LoveHansson/rKolada),
[rTrafa](https://github.com/LoveHansson/rTrafa), and
[pixieweb](https://github.com/LoveHansson/pixieweb).

## Why

The three nordstat packages each ship a per-package `.rds` cache that works
fine for single-user scripts but falls short in multi-user web applications:

- No concurrent-write safety
- Cache granularity is the whole query, not the individual value — two
  overlapping queries refetch the same cells
- Metadata is cached but actual data values (`get_values()`, `get_data()`)
  are not

nordstatExtras replaces this with a single SQLite database shared across
processes. Values are stored at cell granularity, deduplicated across
overlapping queries, and expire based on the age of the cell itself —
a refresh by one query propagates to every query that references the
same cell.

## Install

``` r
# install.packages("devtools")
devtools::install_github("LoveHansson/nordstatExtras")
```

## Use

Open a cache once per deployment, then hand it to the source packages via
their `cache_location` argument:

``` r
library(nordstatExtras)

handle <- nxt_open("cache.sqlite")

# rKolada
kolada_vals <- rKolada::get_values(
  kpi = c("N03700", "N03701"),
  municipality = c("0180", "1480"),
  period = 2020:2024,
  cache = TRUE,
  cache_location = handle
)

# rTrafa
trafa_vals <- rTrafa::get_data(
  "t10011", "itrfslut",
  ar = c("2023", "2024"),
  cache = TRUE,
  cache_location = handle
)

# pixieweb
scb <- pixieweb::px_api("scb")
px_vals <- pixieweb::get_data(
  scb, "BE0101N1",
  Region = c("0180", "1480"),
  Tid = px_top(5),
  cache = TRUE,
  cache_location = handle
)

nxt_close(handle)
```

All three calls store their data in the same SQLite file. On a cache hit
the source package skips the HTTP fetch entirely; on a miss it fetches,
normalizes to cell format, and UPSERTs.

## Design

- **Cells**: the `cells` table stores one row per statistical datum with a
  composite UNIQUE key `(source, api_alias, entity_id, variable, period,
  dims_hash, lang)`. Overlapping queries deduplicate.
- **Dimensions**: arbitrary package-specific dimensions live in a sidecar
  `cell_dims` table, keyed by a hash of the dimension vector.
- **Queries**: the `queries` table plus a `query_cells` junction lets the
  handler look up which cells a given call originally produced, so it can
  reconstruct the source package's expected tibble shape on load.
- **TTL (cell-level)**: a query is fresh iff every cell it expects is
  present and the oldest cell is younger than `max_age` (default 30 days).
  Cross-query refreshes propagate automatically.
- **Async writes**: optional `mirai`-backed background flushing via
  `nxt_write_async()` + `nxt_flush()`. Falls back to sync when `mirai` is
  not installed.
- **Multi-process**: SQLite runs in WAL mode with `synchronous = NORMAL`,
  safe for concurrent readers and serial writers.

## Maintenance

``` r
handle <- nxt_open("cache.sqlite")

# Drop a source entirely
nxt_clear(handle, source = "kolada")

# Delete stale cells (default 30-day TTL)
nxt_gc(handle)

nxt_close(handle)
```
