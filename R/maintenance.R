#' Clear cached queries from the nordstatExtras backend
#'
#' Removes queries (and any cells / metadata rows no longer referenced) from
#' the cache. Filters narrow the scope to a single source, entity, or kind.
#'
#' @param handle An `nxt_handle` from [nxt_open()].
#' @param source Optional source filter (`"kolada"`, `"trafa"`, `"pixieweb"`).
#' @param entity Optional entity filter.
#' @param kind Optional kind filter (`"data"` or `"metadata"`). `NULL`
#'   (default) clears both kinds.
#'
#' @return `invisible(NULL)`. Emits a message summarising what was removed.
#' @export
nxt_clear <- function(handle, source = NULL, entity = NULL, kind = NULL) {
  stopifnot(inherits(handle, "nxt_handle"))
  con <- handle$con

  where <- character(0)
  params <- list()
  if (!is.null(source)) {
    where <- c(where, "source = ?")
    params <- c(params, list(source))
  }
  if (!is.null(entity)) {
    where <- c(where, "entity = ?")
    params <- c(params, list(entity))
  }
  if (!is.null(kind)) {
    where <- c(where, "kind = ?")
    params <- c(params, list(kind))
  }
  where_sql <- if (length(where) > 0) {
    paste("WHERE", paste(where, collapse = " AND "))
  } else {
    ""
  }

  DBI::dbWithTransaction(con, {
    hashes <- DBI::dbGetQuery(
      con, paste("SELECT query_hash FROM queries", where_sql),
      params = params
    )
    n_q <- nrow(hashes)

    if (n_q > 0) {
      placeholders <- paste(rep("?", n_q), collapse = ",")
      DBI::dbExecute(
        con,
        sprintf("DELETE FROM query_cells WHERE query_hash IN (%s);",
                placeholders),
        params = as.list(hashes$query_hash)
      )
      DBI::dbExecute(
        con,
        sprintf("DELETE FROM queries WHERE query_hash IN (%s);",
                placeholders),
        params = as.list(hashes$query_hash)
      )
    }

    n_cells <- DBI::dbExecute(
      con,
      "DELETE FROM cells
         WHERE cell_id NOT IN (SELECT cell_id FROM query_cells);"
    )
    DBI::dbExecute(
      con,
      "DELETE FROM cell_dims
         WHERE dims_hash NOT IN (SELECT dims_hash FROM cells);"
    )

    # Drop orphan search rows (their parent query_hash is gone).
    n_search <- DBI::dbExecute(
      con,
      "DELETE FROM meta_search
         WHERE query_hash NOT IN (SELECT query_hash FROM queries);"
    )
    if (n_search > 0 && nxt_has_fts5(con)) {
      DBI::dbExecute(
        con,
        "INSERT INTO meta_search_fts(meta_search_fts) VALUES('rebuild');"
      )
    }

    inform(sprintf(
      "Removed %d quer%s, %d orphaned cell%s, %d search row%s.",
      n_q, if (n_q == 1) "y" else "ies",
      n_cells, if (n_cells == 1) "" else "s",
      n_search, if (n_search == 1) "" else "s"
    ))
  })

  invisible(NULL)
}

#' Garbage-collect stale cache rows
#'
#' Branches on `queries.kind`:
#'
#' - `kind='data'`: deletes individual cells older than `max_age_days`, drops
#'   junction rows pointing at them, then removes queries that lost every
#'   cell. Queries that still have at least one fresh cell are kept.
#' - `kind='metadata'`: deletes queries whose `fetched_at` is older than
#'   `max_age_days` (query-level TTL — metadata blobs are opaque and can't
#'   benefit from cross-query freshness).
#'
#' Finally prunes orphan rows from `cell_dims` and `meta_search`.
#'
#' @param handle An `nxt_handle`.
#' @param max_age_days Maximum age in days. Default 30.
#' @return `invisible(NULL)`.
#' @export
nxt_gc <- function(handle, max_age_days = NXT_DEFAULT_TTL_DAYS) {
  stopifnot(inherits(handle, "nxt_handle"))
  cutoff <- nxt_now() - as.integer(max_age_days) * 86400L
  con <- handle$con

  DBI::dbWithTransaction(con, {
    # --- data path: cell-level TTL ---
    n_cells <- DBI::dbExecute(
      con, "DELETE FROM cells WHERE fetched_at < ?;",
      params = list(cutoff)
    )
    DBI::dbExecute(
      con,
      "DELETE FROM query_cells
         WHERE cell_id NOT IN (SELECT cell_id FROM cells);"
    )
    n_data_queries <- DBI::dbExecute(
      con,
      "DELETE FROM queries
         WHERE kind = 'data'
           AND n_expected > 0
           AND query_hash NOT IN (SELECT DISTINCT query_hash FROM query_cells);"
    )
    DBI::dbExecute(
      con,
      "DELETE FROM cell_dims
         WHERE dims_hash NOT IN (SELECT dims_hash FROM cells);"
    )

    # --- metadata path: query-level TTL ---
    n_meta_queries <- DBI::dbExecute(
      con,
      "DELETE FROM queries
         WHERE kind = 'metadata' AND fetched_at < ?;",
      params = list(cutoff)
    )

    # --- search index cleanup ---
    n_search <- DBI::dbExecute(
      con,
      "DELETE FROM meta_search
         WHERE query_hash NOT IN (SELECT query_hash FROM queries);"
    )
    if (n_search > 0 && nxt_has_fts5(con)) {
      DBI::dbExecute(
        con,
        "INSERT INTO meta_search_fts(meta_search_fts) VALUES('rebuild');"
      )
    }

    total_queries <- n_data_queries + n_meta_queries
    inform(sprintf(
      "GC removed %d stale cell%s, %d orphan quer%s (%d data, %d metadata), %d search row%s.",
      n_cells, if (n_cells == 1) "" else "s",
      total_queries, if (total_queries == 1) "y" else "ies",
      n_data_queries, n_meta_queries,
      n_search, if (n_search == 1) "" else "s"
    ))
  })
  invisible(NULL)
}
