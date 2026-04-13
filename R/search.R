# Search index for metadata. Decouples storage (whole-BLOB, lossless) from
# search (narrow lossy view, FTS5-indexed). Populated as a side effect of
# every nxt_store_meta() call.
#
# The registry below translates each source's metadata tibble into a small
# (entity_id, title, description) tibble that feeds the `meta_search` table.
# Only entity types a user would actually search by are registered — filter-
# ish entities like `municipality`, `ou`, and `kpi_groups` are deliberately
# omitted so they never clutter typeahead results.
#
# Adding a new searchable entity: drop a function into the appropriate
# source's list. No schema change, no migration.

META_SEARCH_EXTRACTORS <- list(
  kolada = list(
    kpi = function(df) {
      if (!tibble::is_tibble(df) && !is.data.frame(df)) return(NULL)
      if (nrow(df) == 0) return(NULL)
      tibble::tibble(
        entity_id   = as.character(df$id),
        title       = as.character(df$title %||% NA_character_),
        description = as.character(df$description %||% NA_character_)
      )
    }
  ),

  trafa = list(
    products = function(df) {
      if (!tibble::is_tibble(df) && !is.data.frame(df)) return(NULL)
      if (nrow(df) == 0) return(NULL)
      tibble::tibble(
        entity_id   = as.character(df$name),
        title       = as.character(df$label %||% NA_character_),
        description = as.character(df$description %||% NA_character_)
      )
    },
    measures = function(df) {
      if (!tibble::is_tibble(df) && !is.data.frame(df)) return(NULL)
      if (nrow(df) == 0) return(NULL)
      tibble::tibble(
        entity_id   = paste(df$product, df$name, sep = "\x1f"),
        title       = as.character(df$label %||% NA_character_),
        description = as.character(df$description %||% NA_character_)
      )
    }
  ),

  pixieweb = list(
    tables = function(df) {
      if (!tibble::is_tibble(df) && !is.data.frame(df)) return(NULL)
      if (nrow(df) == 0) return(NULL)
      tibble::tibble(
        entity_id   = as.character(df$id),
        title       = as.character(df$title %||% NA_character_),
        description = as.character(df$description %||% NA_character_)
      )
    },
    enriched = function(df) {
      if (!tibble::is_tibble(df) && !is.data.frame(df)) return(NULL)
      if (nrow(df) == 0) return(NULL)
      desc <- trimws(paste(
        df$description %||% "",
        df$contents    %||% "",
        sep = " - "
      ))
      desc[desc == "-" | desc == ""] <- NA_character_
      tibble::tibble(
        entity_id   = as.character(df$id),
        title       = as.character(df$title %||% NA_character_),
        description = desc
      )
    },
    enriched_row = function(df) {
      # table_enrich() stores one row per table_id. Same shape as `enriched`
      # but the caller uses a different entity label so the per-row path is
      # distinguishable in storage. Search index treats both identically.
      META_SEARCH_EXTRACTORS$pixieweb$enriched(df)
    }
  )
)

# Internal: given a raw payload (tibble or list), look up the extractor for
# (source, entity) and UPSERT the resulting rows into `meta_search`. Called
# from within nxt_store_meta()'s transaction.
#
# For payloads that aren't tibbles (e.g. rTrafa's get_structure_raw which
# returns a list of StructureItems), the extractor registry will not have
# an entry and we silently skip — the BLOB is still stored, search just
# won't surface it.
populate_search_index <- function(con, source, entity, payload, query_hash) {
  extractor <- META_SEARCH_EXTRACTORS[[source]][[entity]]
  if (is.null(extractor)) return(invisible(NULL))

  rows <- tryCatch(extractor(payload), error = function(e) {
    warn(sprintf(
      "search extractor for %s/%s failed: %s", source, entity,
      conditionMessage(e)
    ))
    NULL
  })
  if (is.null(rows) || nrow(rows) == 0) return(invisible(NULL))

  # Drop previously-indexed rows from this query so a narrowed query
  # (e.g. get_kpi(id="X") after get_kpi(id=c("X","Y","Z"))) doesn't leave
  # orphans keyed to the old query_hash. Other queries that touched the
  # same entity_id keep their own rows — meta_search's PK is on the
  # entity, so the UPSERT below will transfer ownership to the new query.
  DBI::dbExecute(
    con,
    "DELETE FROM meta_search WHERE query_hash = ?;",
    params = list(query_hash)
  )

  upsert_stmt <- DBI::dbSendStatement(
    con,
    "INSERT INTO meta_search
       (source, entity_type, entity_id, title, description, query_hash)
     VALUES (?, ?, ?, ?, ?, ?)
     ON CONFLICT(source, entity_type, entity_id) DO UPDATE SET
       title       = excluded.title,
       description = excluded.description,
       query_hash  = excluded.query_hash;"
  )
  on.exit(DBI::dbClearResult(upsert_stmt), add = TRUE)

  for (i in seq_len(nrow(rows))) {
    DBI::dbBind(upsert_stmt, list(
      source, entity, rows$entity_id[i],
      rows$title[i], rows$description[i], query_hash
    ))
  }
  DBI::dbClearResult(upsert_stmt)
  on.exit()

  # Refresh the FTS5 external-content index. SQLite's external-content
  # virtual table doesn't auto-sync — we rebuild the affected rows
  # explicitly via the special INSERT ... VALUES('delete', ...) + INSERT
  # pattern. For MVP simplicity we just rebuild the whole FTS index after
  # any write; it's O(n) but n is ~10k rows so <10 ms.
  if (nxt_has_fts5(con)) {
    DBI::dbExecute(
      con,
      "INSERT INTO meta_search_fts(meta_search_fts) VALUES('rebuild');"
    )
  }

  invisible(nrow(rows))
}

#' Full-text search across cached metadata
#'
#' Runs a typeahead-suitable search against the `meta_search` FTS5 index.
#' Every call to a metadata-caching function (e.g. `get_kpi()`,
#' `get_products()`, `get_tables()`) populates this index as a side effect,
#' so `nxt_search()` returns matches across all three source packages in a
#' single query.
#'
#' @param handle An `nxt_handle` from [nxt_open()].
#' @param query FTS5 query string. Supports prefix match via `term*`,
#'   phrase match via `"exact phrase"`, and boolean operators `AND`/`OR`.
#' @param sources Optional character vector restricting results to
#'   specific sources (e.g. `c("kolada", "pixieweb")`).
#' @param entity_types Optional character vector restricting results to
#'   specific entity types (e.g. `c("kpi", "tables")`).
#' @param limit Maximum number of results to return. Default 50.
#'
#' @return A tibble with columns `source`, `entity_type`, `entity_id`,
#'   `title`, `description`, and `rank` (lower = better match). Empty
#'   tibble if nothing matches.
#'
#' @export
nxt_search <- function(handle, query, sources = NULL, entity_types = NULL,
                       limit = 50L) {
  stopifnot(inherits(handle, "nxt_handle"))
  stopifnot(is.character(query), length(query) == 1, nzchar(query))
  con <- handle$con

  empty_result <- tibble::tibble(
    source = character(), entity_type = character(),
    entity_id = character(), title = character(),
    description = character(), rank = numeric()
  )

  if (nxt_has_fts5(con)) {
    # FTS5 path. The JOIN to meta_search is needed because FTS5
    # external-content only materializes the indexed columns (title,
    # description) — we want entity_id and source too.
    where <- "meta_search_fts MATCH ?"
    params <- list(query)

    if (!is.null(sources)) {
      where <- paste0(where, " AND ms.source IN (",
                      paste(rep("?", length(sources)), collapse = ","), ")")
      params <- c(params, as.list(sources))
    }
    if (!is.null(entity_types)) {
      where <- paste0(where, " AND ms.entity_type IN (",
                      paste(rep("?", length(entity_types)), collapse = ","), ")")
      params <- c(params, as.list(entity_types))
    }
    params <- c(params, list(as.integer(limit)))

    sql <- sprintf(
      "SELECT ms.source, ms.entity_type, ms.entity_id,
              ms.title, ms.description, meta_search_fts.rank
         FROM meta_search_fts
         JOIN meta_search ms ON ms.rowid = meta_search_fts.rowid
         WHERE %s
         ORDER BY meta_search_fts.rank
         LIMIT ?;",
      where
    )

    hits <- tryCatch(
      DBI::dbGetQuery(con, sql, params = params),
      error = function(e) {
        warn(sprintf("nxt_search FTS5 query failed: %s", conditionMessage(e)))
        NULL
      }
    )
    if (is.null(hits)) return(empty_result)
    return(tibble::as_tibble(hits))
  }

  # LIKE fallback for SQLite builds without FTS5. Prefix match only;
  # slower (full scan) but correct for small catalogs.
  if (!isTRUE(nxt_state$warned_no_fts5)) {
    warn("FTS5 unavailable; nxt_search() falling back to LIKE prefix match.")
    nxt_state$warned_no_fts5 <- TRUE
  }

  needle <- paste0(gsub("\\*", "", query), "%")

  where <- "(title LIKE ? OR description LIKE ?)"
  params <- list(needle, needle)

  if (!is.null(sources)) {
    where <- paste0(where, " AND source IN (",
                    paste(rep("?", length(sources)), collapse = ","), ")")
    params <- c(params, as.list(sources))
  }
  if (!is.null(entity_types)) {
    where <- paste0(where, " AND entity_type IN (",
                    paste(rep("?", length(entity_types)), collapse = ","), ")")
    params <- c(params, as.list(entity_types))
  }
  params <- c(params, list(as.integer(limit)))

  sql <- sprintf(
    "SELECT source, entity_type, entity_id, title, description,
            0.0 AS rank
       FROM meta_search
       WHERE %s
       LIMIT ?;",
    where
  )

  hits <- DBI::dbGetQuery(con, sql, params = params)
  tibble::as_tibble(hits)
}
