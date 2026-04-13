#' Open or create a nordstatExtras SQLite cache
#'
#' Opens a SQLite database at `path`, creating the file and applying the
#' schema if it does not yet exist. Sets WAL mode and pragmas suitable for
#' multi-process read/write access.
#'
#' @param path Path to the `.sqlite` file.
#' @param create Logical. If `FALSE`, `nxt_open()` fails when the file does
#'   not exist. Default `TRUE`.
#'
#' @return An object of class `nxt_handle` — a thin wrapper around the DBI
#'   connection that also carries the path and async-queue identity.
#'
#' @examples
#' \donttest{
#' path <- tempfile(fileext = ".sqlite")
#' handle <- nxt_open(path)
#' print(handle)
#' nxt_close(handle)
#' unlink(path)
#' }
#'
#' @export
nxt_open <- function(path, create = TRUE) {
  if (!is.character(path) || length(path) != 1) {
    abort("`path` must be a single string.")
  }
  if (!file.exists(path) && !isTRUE(create)) {
    abort(sprintf("Cache file does not exist: %s", path))
  }

  dir <- dirname(path)
  if (!dir.exists(dir)) dir.create(dir, recursive = TRUE)

  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  nxt_apply_schema(con)

  structure(
    list(con = con, path = normalizePath(path, mustWork = FALSE)),
    class = "nxt_handle"
  )
}

#' Close a nordstatExtras cache handle
#'
#' Flushes any pending async writes and disconnects the underlying DBI
#' connection.
#'
#' @param handle A handle returned by [nxt_open()].
#' @return `invisible(NULL)`
#' @examples
#' \donttest{
#' path <- tempfile(fileext = ".sqlite")
#' handle <- nxt_open(path)
#' nxt_close(handle)
#' unlink(path)
#' }
#' @export
nxt_close <- function(handle) {
  stopifnot(inherits(handle, "nxt_handle"))
  try(nxt_flush(handle), silent = TRUE)
  try(DBI::dbDisconnect(handle$con), silent = TRUE)
  invisible(NULL)
}

#' Detect whether a cache location should use the nordstatExtras backend
#'
#' Returns `TRUE` when `cache_location` points at a `.sqlite`/`.db` file or
#' an existing `nxt_handle`. Used by rKolada/rTrafa/pixieweb inside their
#' `cache_handler()` functions to decide whether to delegate.
#'
#' @param cache_location A path, function returning a path, or `nxt_handle`.
#' @return Logical scalar.
#' @examples
#' nxt_is_backend("my_cache.sqlite")   # TRUE
#' nxt_is_backend("my_cache.db")       # TRUE
#' nxt_is_backend("/tmp/cache_dir")    # FALSE
#' nxt_is_backend(tempdir())           # FALSE
#' @export
nxt_is_backend <- function(cache_location) {
  if (inherits(cache_location, "nxt_handle")) return(TRUE)
  if (is.function(cache_location)) cache_location <- tryCatch(
    cache_location(), error = function(e) NULL
  )
  if (!is.character(cache_location) || length(cache_location) != 1) {
    return(FALSE)
  }
  grepl("\\.(sqlite3?|db)$", cache_location, ignore.case = TRUE)
}

# Internal: accept either a handle or a path/function, return a handle.
as_handle <- function(x) {
  if (inherits(x, "nxt_handle")) return(x)
  if (is.function(x)) x <- x()
  nxt_open(x, create = TRUE)
}

#' @export
print.nxt_handle <- function(x, ...) {
  cat("<nxt_handle>\n")
  cat("  path: ", x$path, "\n", sep = "")
  alive <- tryCatch(DBI::dbIsValid(x$con), error = function(e) FALSE)
  cat("  open: ", alive, "\n", sep = "")
  invisible(x)
}
