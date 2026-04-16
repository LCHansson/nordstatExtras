# recorded_search.R — persisting cross-title search keywords
#
# Some source APIs (notably PX-Web) match on more than the visible title
# and description of a table. A search for "punktlighet" on SCB returns
# tables whose titles don't contain that word at all — the API uses
# richer server-side fields we don't mirror locally. Without this layer,
# an identical search run against the local FTS5 index returns zero hits
# and the user has to hit the live-search "nödutgång" again every time.
#
# nxt_record_search() closes that loop: after a live search surfaces a
# set of entities, we remember the (query -> entity_ids) association by
# appending the query term to each entity's `search_keywords` column and
# UPSERTing the query itself into `recorded_searches` so cron can refresh
# it before it goes stale.

#' Record a live search so the local index finds it next time
#'
#' Appends `query` to the `search_keywords` column of every entity in
#' `entity_ids` (deduplicated, whitespace-separated), refreshes the FTS5
#' index, and UPSERTs the query into `recorded_searches` with a `last_run_at`
#' timestamp. The entities must already exist in `meta_search` — call the
#' source-specific `get_*` functions with the nordstatExtras cache first.
#'
#' @param handle `<nxt_handle>` from [nxt_open()].
#' @param query Character scalar — the user's search term (kept verbatim
#'   in `recorded_searches`; added as a token to `search_keywords`).
#' @param source Character scalar — one of `"kolada"`, `"pixieweb"`,
#'   `"trafa"`.
#' @param entity_ids Character vector of entity ids to associate with the
#'   query (from the respective `get_*` output's `id`/`name` column).
#' @param ttl_days Integer — max age before [nxt_expired_searches()]
#'   returns this search for re-running. Default 30.
#' @return Invisibly, the number of meta_search rows updated.
#' @export
nxt_record_search <- function(handle, query, source, entity_ids,
                              ttl_days = 30L) {
  stopifnot(inherits(handle, "nxt_handle"))
  stopifnot(is.character(query), length(query) == 1, nzchar(query))
  stopifnot(is.character(source), length(source) == 1, nzchar(source))
  stopifnot(is.character(entity_ids))

  con <- handle$con
  now <- as.integer(Sys.time())
  q_token <- .search_keywords_token(query)

  # UPSERT the query into recorded_searches so cron can re-run it later.
  # We overwrite last_run_at so "known, still fresh" beats "known but
  # stale" when the same query comes back from a different client.
  DBI::dbExecute(
    con,
    "INSERT INTO recorded_searches
       (query, source, last_run_at, ttl_days, last_hit_count)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(query, source) DO UPDATE SET
       last_run_at    = excluded.last_run_at,
       ttl_days       = excluded.ttl_days,
       last_hit_count = excluded.last_hit_count;",
    params = list(query, source, now, as.integer(ttl_days),
                  length(entity_ids))
  )

  if (length(entity_ids) == 0) return(invisible(0L))

  # For each entity, append the token to search_keywords if it isn't
  # already present. meta_search.PRIMARY KEY is (source, entity_type,
  # entity_id) — we don't know entity_type here, but a single query
  # shouldn't legitimately span multiple types within a source, so we
  # update whichever row(s) exist. The ON CONFLICT on entity ignores
  # missing rows (they simply aren't in the index; caller should prime
  # the source first).
  updated <- 0L
  for (eid in entity_ids) {
    res <- DBI::dbExecute(
      con,
      "UPDATE meta_search
         SET search_keywords = TRIM(
             CASE
               WHEN search_keywords = '' THEN ?
               WHEN INSTR(' ' || search_keywords || ' ', ' ' || ? || ' ') > 0
                 THEN search_keywords
               ELSE search_keywords || ' ' || ?
             END)
       WHERE source = ? AND entity_id = ?;",
      params = list(q_token, q_token, q_token, source, eid)
    )
    updated <- updated + res
  }

  # Refresh the FTS5 index so the new keywords are searchable. Same
  # whole-index rebuild strategy as populate_search_index() — O(n) but
  # cheap on our data volumes.
  if (nxt_has_fts5(con) && updated > 0) {
    DBI::dbExecute(
      con,
      "INSERT INTO meta_search_fts(meta_search_fts) VALUES('rebuild');"
    )
  }

  invisible(as.integer(updated))
}

#' List recorded searches that are due for a refresh
#'
#' Returns the `(query, source)` pairs whose `last_run_at` is older than
#' `max_age` days (or their own `ttl_days` entry, whichever is smaller).
#' A cron job can iterate the result and re-run each search against the
#' live API, passing the fresh entity list back to [nxt_record_search()].
#'
#' @param handle `<nxt_handle>` from [nxt_open()].
#' @param max_age Default maximum age in days when a row's own
#'   `ttl_days` is NULL or smaller. Default 30.
#' @return A tibble with columns `query`, `source`, `last_run_at`
#'   (POSIXct), `ttl_days`, `last_hit_count`. Ordered oldest-first.
#' @export
nxt_expired_searches <- function(handle, max_age = 30L) {
  stopifnot(inherits(handle, "nxt_handle"))
  stopifnot(is.numeric(max_age), length(max_age) == 1, max_age > 0)

  con <- handle$con
  cutoff_now <- as.integer(Sys.time())
  max_age_secs <- as.integer(max_age * 86400L)

  rows <- DBI::dbGetQuery(
    con,
    "SELECT query, source, last_run_at, ttl_days, last_hit_count
       FROM recorded_searches
      WHERE (? - last_run_at) >= MIN(ttl_days * 86400, ?)
      ORDER BY last_run_at ASC;",
    params = list(cutoff_now, max_age_secs)
  )

  tibble::tibble(
    query           = rows$query,
    source          = rows$source,
    last_run_at     = as.POSIXct(rows$last_run_at, origin = "1970-01-01"),
    ttl_days        = as.integer(rows$ttl_days),
    last_hit_count  = as.integer(rows$last_hit_count)
  )
}

# Internal helper: sanitize a query into a single FTS5-safe token. Strips
# whitespace, lower-cases, keeps alphanumerics + Swedish diacritics. Empty
# strings return "" and are handled by callers.
.search_keywords_token <- function(query) {
  q <- tolower(query)
  q <- gsub("[[:space:]]+", "_", q)
  # Drop anything that would confuse FTS5's tokenizer. The
  # `unicode61 remove_diacritics 2` tokenizer handles å/ä/ö fine; we
  # just need to avoid quotes / parens / brackets / commas.
  q <- gsub("[^[:alnum:]\u00e5\u00e4\u00f6\u00c5\u00c4\u00d6_-]", "", q)
  q
}
