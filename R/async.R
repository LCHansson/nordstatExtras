# Asynchronous write path. Buffered in a per-handle queue, flushed either
# when the buffer exceeds a threshold, on explicit nxt_flush(), or when the
# handle is closed. Uses `mirai` when available; falls back to synchronous
# writes with a one-time warning otherwise.

NXT_ASYNC_THRESHOLD <- 500L  # cells

# Per-handle state lives in nxt_state$async_queues, keyed by handle$path.
get_queue <- function(handle) {
  k <- handle$path
  q <- nxt_state$async_queues[[k]]
  if (is.null(q)) {
    q <- new.env(parent = emptyenv())
    q$buffer <- list()   # list of pending write payloads
    q$pending <- NULL    # mirai task if one is in flight
    q$warned_no_mirai <- FALSE
    nxt_state$async_queues[[k]] <- q
  }
  q
}

mirai_available <- function() {
  requireNamespace("mirai", quietly = TRUE)
}

#' Enqueue an asynchronous cache write
#'
#' Buffers a write for a specific `(source, entity, key_params)` query and
#' returns immediately. The actual UPSERT into SQLite happens either in a
#' background `mirai` task or synchronously when the package isn't available.
#'
#' @param handle An `nxt_handle` from [nxt_open()].
#' @param source,entity,key_params Identity of the query (same as
#'   [nxt_cache_handler()]).
#' @param df The tibble to store.
#' @param normalize_extra Named list of extra args for the normalizer.
#'
#' @return `invisible(handle)`, for pipe-friendliness.
#' @export
nxt_write_async <- function(handle, source, entity, key_params, df,
                            normalize_extra = list()) {
  stopifnot(inherits(handle, "nxt_handle"))
  q <- get_queue(handle)

  q$buffer[[length(q$buffer) + 1]] <- list(
    source = source, entity = entity, key_params = key_params,
    df = df, normalize_extra = normalize_extra,
    query_hash = hash_query(source, entity, key_params)
  )

  buffer_rows <- sum(vapply(q$buffer,
                            function(x) NROW(x$df), integer(1)))
  if (buffer_rows >= NXT_ASYNC_THRESHOLD) {
    flush_buffer(handle, q)
  }
  invisible(handle)
}

#' Flush pending async writes
#'
#' Blocks until all buffered writes for a handle have been committed.
#'
#' @param handle An `nxt_handle`.
#' @return `invisible(NULL)`
#' @export
nxt_flush <- function(handle) {
  stopifnot(inherits(handle, "nxt_handle"))
  q <- get_queue(handle)
  flush_buffer(handle, q, wait = TRUE)
  invisible(NULL)
}

# Internal: drain the buffer. If mirai is available and `wait` is FALSE, the
# work runs in a background task. Otherwise it runs synchronously.
flush_buffer <- function(handle, q, wait = FALSE) {
  if (length(q$buffer) == 0) {
    if (!is.null(q$pending) && wait && mirai_available()) {
      mirai::call_mirai(q$pending)
      q$pending <- NULL
    }
    return(invisible(NULL))
  }

  payload <- q$buffer
  q$buffer <- list()

  if (!mirai_available()) {
    if (!isTRUE(q$warned_no_mirai)) {
      warn("nordstatExtras: `mirai` not installed; async writes will run synchronously.")
      q$warned_no_mirai <- TRUE
    }
    run_flush(handle$path, payload)
    return(invisible(NULL))
  }

  # Wait for a previous task before launching a new one — keeps writes
  # serialized to avoid transaction churn.
  if (!is.null(q$pending)) {
    mirai::call_mirai(q$pending)
    q$pending <- NULL
  }

  # Pass run_flush as a mirai arg so the expression body doesn't need `:::`
  # to reach an internal function, and the worker receives a fully
  # self-contained closure.
  task <- mirai::mirai(
    run_flush(path, payload),
    run_flush = run_flush,
    path = handle$path,
    payload = payload
  )

  if (isTRUE(wait)) {
    mirai::call_mirai(task)
    q$pending <- NULL
  } else {
    q$pending <- task
  }

  invisible(NULL)
}

# Internal: open a short-lived connection from a background task and drain
# a batch of payloads. Kept standalone so it can run inside a `mirai` worker
# without capturing the main-thread handle.
run_flush <- function(path, payload) {
  bg <- nxt_open(path, create = FALSE)
  on.exit(nxt_close(bg), add = TRUE)

  for (p in payload) {
    normalized <- do.call(
      normalize_for_source,
      c(list(source = p$source, df = p$df), p$normalize_extra)
    )
    nxt_store_cells(bg, p$query_hash, p$source, p$entity, normalized)
  }
  invisible(length(payload))
}
