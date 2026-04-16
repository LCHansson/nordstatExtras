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
#' @examples
#' \donttest{
#' path <- tempfile(fileext = ".sqlite")
#' handle <- nxt_open(path)
#'
#' # Populate the search index by storing some metadata
#' ch <- nxt_cache_handler(
#'   source = "kolada", entity = "kpi", cache = TRUE,
#'   cache_location = handle, kind = "metadata",
#'   key_params = list()
#' )
#' ch("store", data.frame(
#'   id = c("N03700", "N01951"),
#'   title = c("Befolkning totalt", "Bruttoregionprodukt"),
#'   description = c("Antal invanare", "BRP per capita")
#' ))
#'
#' # Typeahead search
#' nxt_search(handle, "bef*")
#'
#' # Filter by source
#' nxt_search(handle, "bef*", sources = "kolada")
#'
#' nxt_close(handle)
#' unlink(path)
#' }
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
    # FTS5 path. Two-step approach: (1) get matching rowids from FTS5,
    # (2) look up full rows from meta_search using IN (...). The naive
    # JOIN (meta_search_fts JOIN meta_search ON rowid) is pathologically
    # slow with external-content FTS5 tables — 60+ seconds for 6k rows.

    # Step 1: collect ranked rowids from FTS5 (sub-millisecond)
    fts_where <- "meta_search_fts MATCH ?"
    fts_params <- list(query)

    # We can't filter by source/entity_type in the FTS5 query (those
    # columns aren't in the virtual table). Apply filters in step 2.
    fts_sql <- sprintf(
      "SELECT rowid, rank FROM meta_search_fts WHERE %s
       ORDER BY rank LIMIT ?;",
      fts_where
    )
    # Fetch more than needed to compensate for post-filter loss
    fts_limit <- as.integer(limit) * 10L
    fts_params <- c(fts_params, list(fts_limit))

    fts_hits <- tryCatch(
      DBI::dbGetQuery(con, fts_sql, params = fts_params),
      error = function(e) {
        warn(sprintf("nxt_search FTS5 query failed: %s", conditionMessage(e)))
        NULL
      }
    )
    if (is.null(fts_hits) || nrow(fts_hits) == 0) return(empty_result)

    # Step 2: fetch full rows from meta_search by rowid (fast PK lookup)
    rowids <- fts_hits$rowid
    ranks <- fts_hits$rank
    placeholders <- paste(rep("?", length(rowids)), collapse = ",")

    ms_where <- sprintf("rowid IN (%s)", placeholders)
    ms_params <- as.list(rowids)

    if (!is.null(sources)) {
      ms_where <- paste0(ms_where, " AND source IN (",
                         paste(rep("?", length(sources)), collapse = ","), ")")
      ms_params <- c(ms_params, as.list(sources))
    }
    if (!is.null(entity_types)) {
      ms_where <- paste0(ms_where, " AND entity_type IN (",
                         paste(rep("?", length(entity_types)), collapse = ","), ")")
      ms_params <- c(ms_params, as.list(entity_types))
    }

    ms_sql <- sprintf(
      "SELECT rowid, source, entity_type, entity_id, title, description
         FROM meta_search
        WHERE %s;",
      ms_where
    )

    ms_hits <- DBI::dbGetQuery(con, ms_sql, params = ms_params)
    if (nrow(ms_hits) == 0) return(empty_result)

    # Merge rank from FTS5 step and apply limit
    rank_lookup <- stats::setNames(ranks, as.character(rowids))
    ms_hits$rank <- unname(rank_lookup[as.character(ms_hits$rowid)])
    ms_hits <- ms_hits[order(ms_hits$rank), ]
    ms_hits <- utils::head(ms_hits, limit)
    ms_hits$rowid <- NULL

    return(tibble::as_tibble(ms_hits))
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
