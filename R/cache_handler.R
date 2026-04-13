#' Drop-in cache handler for the nordstat family
#'
#' Produces a closure matching the `cache_handler()` contract used by
#' rKolada, rTrafa, and pixieweb: a function of `(method, df)` where `method`
#' is one of `"discover"`, `"load"`, or `"store"`. Unlike the per-package
#' `.rds` handlers, this one stores values in a shared SQLite file. Two kinds
#' of cached content are supported:
#'
#' - `kind = "data"` (default) — statistical data stored at cell granularity
#'   with cross-query freshness. Matches the existing rKolada `get_values()`
#'   / rTrafa `get_data()` / pixieweb `get_data()` integration.
#' - `kind = "metadata"` — whole serialized objects (tibbles or lists) stored
#'   as BLOBs on the `queries` row. Matches the metadata-caching integration
#'   for `get_kpi()`, `get_products()`, `get_tables()`, etc. TTL is query-level.
#'
#' @param source One of `"kolada"`, `"trafa"`, `"pixieweb"`.
#' @param entity Entity keyword (e.g. `"values"`, `"products"`, `"tables"`).
#' @param cache Logical — whether caching is enabled.
#' @param cache_location Either an `nxt_handle` from [nxt_open()] or a path /
#'   function-returning-a-path to a `.sqlite` file.
#' @param key_params Named list of parameters that together with `source` +
#'   `entity` form a unique cache key.
#' @param kind Either `"data"` (default, cell-level) or `"metadata"`
#'   (whole-BLOB). Determines the storage/TTL strategy.
#' @param ttl_days Time-to-live in days. Defaults to 30.
#' @param reconstruct_extra Named list of extra arguments forwarded to the
#'   reconstruct function — used when trafa/pixieweb need product/alias/etc.
#'   at load time. Ignored when `kind = "metadata"`.
#' @param normalize_extra Same for the normalize function, used at store time.
#'   Ignored when `kind = "metadata"` — metadata is serialized whole.
#'
#' @return A function with signature `function(method, df = NULL)`.
#' @export
nxt_cache_handler <- function(source, entity, cache, cache_location,
                              key_params = list(),
                              kind = c("data", "metadata"),
                              ttl_days = NXT_DEFAULT_TTL_DAYS,
                              reconstruct_extra = list(),
                              normalize_extra = list()) {
  kind <- match.arg(kind)

  if (!isTRUE(cache)) {
    return(function(method, df = NULL) {
      if (method == "store") return(df)
      FALSE
    })
  }

  handle <- as_handle(cache_location)
  query_hash <- hash_query(source, entity, key_params)
  max_age <- as.integer(ttl_days) * 86400L

  function(method, df = NULL) {
    switch(method,
      discover = {
        !is.null(nxt_lookup_query(handle, query_hash, max_age,
                                  expected_kind = kind))
      },
      load = {
        meta <- nxt_lookup_query(handle, query_hash, max_age,
                                 expected_kind = kind)
        if (is.null(meta)) return(NULL)
        if (kind == "metadata") {
          return(nxt_load_meta(handle, query_hash))
        }
        cells <- nxt_load_cells(handle, query_hash)
        reconstruct_source(source, cells, meta$template)
      },
      store = {
        if (kind == "metadata") {
          nxt_store_meta(handle, query_hash, source, entity, df)
          return(df)
        }
        normalized <- do.call(
          normalize_for_source,
          c(list(source = source, df = df), normalize_extra)
        )
        nxt_store_cells(handle, query_hash, source, entity, normalized)
        df
      },
      NULL
    )
  }
}

# Dispatcher — calls the right normalize_*() for a given source.
normalize_for_source <- function(source, df, ...) {
  switch(source,
    kolada   = normalize_kolada(df, ...),
    trafa    = normalize_trafa(df, ...),
    pixieweb = normalize_pixieweb(df, ...),
    abort(sprintf("normalize_for_source: unknown source '%s'", source))
  )
}
