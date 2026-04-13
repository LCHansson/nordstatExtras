#' @importFrom rlang %||% abort inform warn
NULL

# Default TTL in days for cached queries.
NXT_DEFAULT_TTL_DAYS <- 30L

# Sentinel for "no value" in UNIQUE-significant columns. SQLite treats NULLs
# as distinct inside UNIQUE constraints, so we use an empty string instead.
NXT_NULL_SENTINEL <- ""

# Package-level environment for state that must survive between function calls
# within a single R process (async queue, open handles, one-shot warnings).
nxt_state <- new.env(parent = emptyenv())
nxt_state$async_queues <- list()
nxt_state$warned_no_fts5 <- FALSE

# Current UNIX time (integer seconds). Wrapped so tests can stub it.
nxt_now <- function() {
  as.integer(Sys.time())
}

# Coerce NULL/NA/empty to the NULL sentinel for column values that take part
# in the cells UNIQUE constraint.
coerce_sentinel <- function(x) {
  if (is.null(x)) return(NXT_NULL_SENTINEL)
  x <- as.character(x)
  x[is.na(x) | !nzchar(x)] <- NXT_NULL_SENTINEL
  x
}

# Stable hash over a named list of dimension (name, value) pairs. Used to
# identify cells with the same "dimension vector" across different queries.
hash_dims <- function(dims) {
  if (length(dims) == 0) return(digest::digest("", algo = "sha1"))
  dims <- dims[order(names(dims))]
  blob <- paste(names(dims), vapply(dims, as.character, character(1)),
                sep = "=", collapse = ";")
  substr(digest::digest(blob, algo = "sha1"), 1, 16)
}

# Stable hash for a whole query: (source, entity, sorted key_params).
# Handles empty and NULL key_params safely — empty queries still hash
# to a stable value so a call like `get_kpi()` (no filters) has a cache key.
hash_query <- function(source, entity, key_params) {
  parts <- if (length(key_params) > 0 && !is.null(names(key_params))) {
    key_params <- key_params[order(names(key_params))]
    paste(
      names(key_params),
      vapply(key_params, function(x) paste(as.character(x), collapse = ","),
             character(1)),
      sep = "=", collapse = ";"
    )
  } else {
    ""
  }
  blob <- paste(source, entity, parts, sep = "|")
  substr(digest::digest(blob, algo = "sha1"), 1, 16)
}
