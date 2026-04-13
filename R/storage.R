# Low-level read/write against the cells + queries tables. Used by both the
# cache_handler drop-in and the async write path.
#
# Performance note: for large batches (tens of thousands of rows) the hot
# path here uses a temporary staging table rather than row-by-row dbBind.
# We write the batch once via dbWriteTable, then use INSERT … SELECT … ON
# CONFLICT to UPSERT all rows in a single SQL statement, and a second
# INSERT … SELECT (joining cells with the staging table on the natural key)
# to rebuild query_cells. This collapses O(n) round-trips to a handful of
# statements regardless of batch size.

# Write a normalized batch (cells + template) to the DB under a given query
# hash. Atomic: all operations happen inside a single transaction.
nxt_store_cells <- function(handle, query_hash, source, entity,
                            normalized, fetched_at = nxt_now()) {
  con <- handle$con
  cells <- normalized$cells
  template <- normalized$template

  # Pre-serialize the template once — used regardless of whether the batch
  # is empty, inside the transaction below.
  df_template_json <- jsonlite::toJSON(template, auto_unbox = TRUE,
                                       null = "null")

  DBI::dbWithTransaction(con, {
    n_expected <- 0L

    if (nrow(cells) > 0) {
      # 1. Stage the batch in a temporary table. dbWriteTable is vectorized
      #    and uses SQLite's bulk insert path — orders of magnitude faster
      #    than per-row dbBind for large tibbles.
      staging <- data.frame(
        source     = cells$source,
        api_alias  = cells$api_alias,
        entity_id  = cells$entity_id,
        variable   = cells$variable,
        period     = cells$period,
        dims_hash  = cells$dims_hash,
        value_num  = cells$value_num,
        value_txt  = cells$value_txt,
        status     = cells$status,
        lang       = cells$lang,
        fetched_at = fetched_at,
        stringsAsFactors = FALSE
      )
      # Use a transaction-scoped name; overwrite in case a prior aborted
      # transaction left it around.
      DBI::dbExecute(con, "DROP TABLE IF EXISTS nxt_tmp_cells_batch;")
      DBI::dbWriteTable(con, "nxt_tmp_cells_batch", staging,
                        temporary = TRUE)

      # 2. UPSERT from the staging table into cells. One statement, no
      #    R → SQLite round-trips per row.
      #
      # The `WHERE true` is not a no-op: it disambiguates the SELECT from
      # the following `ON CONFLICT` clause. Without it, SQLite's parser
      # treats `ON` as the start of a JOIN clause and fails with
      # "near DO: syntax error". This is a documented SQLite quirk for
      # INSERT … SELECT … ON CONFLICT.
      DBI::dbExecute(con, "
        INSERT INTO cells
          (source, api_alias, entity_id, variable, period, dims_hash,
           value_num, value_txt, status, lang, fetched_at)
        SELECT source, api_alias, entity_id, variable, period, dims_hash,
               value_num, value_txt, status, lang, fetched_at
          FROM nxt_tmp_cells_batch
         WHERE true
        ON CONFLICT (source, api_alias, entity_id, variable, period,
                     dims_hash, lang)
        DO UPDATE SET
          value_num  = excluded.value_num,
          value_txt  = excluded.value_txt,
          status     = excluded.status,
          fetched_at = excluded.fetched_at;
      ")

      # 3. Stage + bulk-insert cell_dims rows. collect_dim_rows returns a
      #    deduplicated frame; PRIMARY KEY (dims_hash, dim_name) handles
      #    any remaining conflicts via INSERT OR REPLACE.
      dim_rows <- collect_dim_rows(cells)
      if (nrow(dim_rows) > 0) {
        DBI::dbExecute(con, "DROP TABLE IF EXISTS nxt_tmp_dims_batch;")
        DBI::dbWriteTable(con, "nxt_tmp_dims_batch",
                          as.data.frame(dim_rows), temporary = TRUE)
        DBI::dbExecute(con, "
          INSERT OR REPLACE INTO cell_dims
            (dims_hash, dim_name, dim_code, dim_label)
          SELECT dims_hash, dim_name, dim_code, dim_label
            FROM nxt_tmp_dims_batch;
        ")
        DBI::dbExecute(con, "DROP TABLE nxt_tmp_dims_batch;")
      }

      # 4. Rebuild query_cells for this query_hash. The join on the full
      #    natural key yields exactly the cell_ids we just UPSERTed.
      DBI::dbExecute(con, "DELETE FROM query_cells WHERE query_hash = ?;",
                     params = list(query_hash))
      DBI::dbExecute(con, "
        INSERT OR IGNORE INTO query_cells (query_hash, cell_id)
        SELECT ?, c.cell_id
          FROM cells c
          JOIN nxt_tmp_cells_batch t
            ON t.source    = c.source
           AND t.api_alias = c.api_alias
           AND t.entity_id = c.entity_id
           AND t.variable  = c.variable
           AND t.period    = c.period
           AND t.dims_hash = c.dims_hash
           AND t.lang      = c.lang;
      ", params = list(query_hash))

      # n_expected = number of distinct cells this query references
      n_expected <- DBI::dbGetQuery(
        con,
        "SELECT COUNT(*) AS n FROM query_cells WHERE query_hash = ?;",
        params = list(query_hash)
      )$n[1]

      DBI::dbExecute(con, "DROP TABLE nxt_tmp_cells_batch;")
    }

    # 5. Upsert the queries row.
    DBI::dbExecute(
      con,
      "INSERT INTO queries
         (query_hash, source, entity, kind, fetched_at, n_expected,
          df_template, payload)
       VALUES (?,?,?,'data',?,?,?,NULL)
       ON CONFLICT(query_hash) DO UPDATE SET
         kind        = 'data',
         fetched_at  = excluded.fetched_at,
         n_expected  = excluded.n_expected,
         df_template = excluded.df_template,
         payload     = NULL;",
      params = list(query_hash, source, entity, fetched_at, n_expected,
                    df_template_json)
    )
  })

  invisible(nrow(cells))
}

# Store a metadata payload as a serialized BLOB on the queries row. Unlike
# nxt_store_cells(), there is no cell decomposition — the whole object (tibble
# or list) is serialized via base::serialize() to preserve attributes and list
# columns byte-identically. After the BLOB is written, `populate_search_index()`
# is called to populate the FTS5 side-index; both operations share the same
# transaction so either both succeed or both roll back.
nxt_store_meta <- function(handle, query_hash, source, entity, payload,
                           fetched_at = nxt_now()) {
  con <- handle$con
  raw_blob <- base::serialize(payload, connection = NULL)

  DBI::dbWithTransaction(con, {
    DBI::dbExecute(
      con,
      "INSERT INTO queries
         (query_hash, source, entity, kind, fetched_at, n_expected,
          df_template, payload)
       VALUES (?,?,?,'metadata',?,0,NULL,?)
       ON CONFLICT(query_hash) DO UPDATE SET
         kind        = 'metadata',
         fetched_at  = excluded.fetched_at,
         n_expected  = 0,
         df_template = NULL,
         payload     = excluded.payload;",
      params = list(query_hash, source, entity, fetched_at,
                    list(raw_blob))
    )
    # Clear any stale cells junction rows — shouldn't exist for a metadata
    # hash, but belt-and-suspenders.
    DBI::dbExecute(con, "DELETE FROM query_cells WHERE query_hash = ?;",
                   params = list(query_hash))

    # Populate the FTS5 side-index (R/search.R). No-op if the source/entity
    # combination has no extractor registered.
    populate_search_index(con, source, entity, payload, query_hash)
  })

  invisible(1L)
}

# Load a metadata BLOB back to its original R object. Returns NULL if the
# query isn't present or isn't kind='metadata'.
nxt_load_meta <- function(handle, query_hash) {
  row <- DBI::dbGetQuery(
    handle$con,
    "SELECT payload FROM queries
       WHERE query_hash = ? AND kind = 'metadata';",
    params = list(query_hash)
  )
  if (nrow(row) == 0) return(NULL)
  raw_blob <- row$payload[[1]]
  if (is.null(raw_blob) || length(raw_blob) == 0) return(NULL)
  base::unserialize(raw_blob)
}

# Extract all (dims_hash, dim_name, dim_code, dim_label) triples from the
# list column `dims` on a cells tibble.
#
# Performance-critical: the previous implementation used an O(n²) seen-set
# check via `%in%` on a growing character vector, which became minutes-slow
# on batches >10k rows. This version preallocates three parallel vectors,
# fills them in a single linear sweep, and dedupes at the end via
# `!duplicated(cbind(...))` — O(n) total.
collect_dim_rows <- function(cells) {
  empty <- tibble::tibble(
    dims_hash = character(), dim_name = character(),
    dim_code  = character(), dim_label = character()
  )
  if (nrow(cells) == 0) return(empty)

  total <- sum(vapply(cells$dims, length, integer(1)))
  if (total == 0) return(empty)

  hashes <- character(total)
  names_ <- character(total)
  codes  <- character(total)

  k <- 1L
  for (i in seq_len(nrow(cells))) {
    d <- cells$dims[[i]]
    n <- length(d)
    if (n == 0) next
    idx <- k:(k + n - 1L)
    hashes[idx] <- cells$dims_hash[i]
    names_[idx] <- names(d)
    # `d` is a list of atomic scalars — unlist is vectorized and fast.
    codes[idx]  <- as.character(unlist(d, use.names = FALSE))
    k <- k + n
  }

  # Dedupe on (dims_hash, dim_name). duplicated() on a data.frame of two
  # character columns is vectorized C code — O(n) with a hash table.
  df <- data.frame(dims_hash = hashes, dim_name = names_,
                   dim_code  = codes,  dim_label = NA_character_,
                   stringsAsFactors = FALSE)
  keep <- !duplicated(df[, c("dims_hash", "dim_name"), drop = FALSE])
  tibble::as_tibble(df[keep, , drop = FALSE])
}

# Look up template + TTL status for a query hash. Dispatches on the stored
# `kind` — cell-level TTL for 'data', query-level TTL for 'metadata'.
#
# `expected_kind` (optional) lets the caller assert which kind they expect;
# if the stored kind doesn't match, returns NULL. nxt_cache_handler() sets
# this so a data handler never accidentally loads a metadata row under a
# colliding hash, and vice versa.
#
# Returns NULL if:
#   - the row doesn't exist
#   - expected_kind is set and doesn't match stored kind
#   - for 'data': n_present < n_expected (cells GC'd) or oldest cell stale
#   - for 'metadata': queries.fetched_at is older than max_age_seconds
nxt_lookup_query <- function(handle, query_hash,
                             max_age_seconds = NXT_DEFAULT_TTL_DAYS * 86400L,
                             expected_kind = NULL) {
  meta <- DBI::dbGetQuery(
    handle$con,
    "SELECT source, entity, kind, fetched_at, n_expected, df_template
       FROM queries WHERE query_hash = ?;",
    params = list(query_hash)
  )
  if (nrow(meta) == 0) return(NULL)

  kind <- meta$kind[1]
  if (!is.null(expected_kind) && !identical(kind, expected_kind)) return(NULL)

  if (kind == "metadata") {
    # Query-level TTL: freshness comes from queries.fetched_at.
    age <- nxt_now() - as.integer(meta$fetched_at[1])
    if (age > max_age_seconds) return(NULL)
    return(list(
      source = meta$source[1],
      entity = meta$entity[1],
      kind = "metadata",
      fetched_at = meta$fetched_at[1],
      template = NULL
    ))
  }

  # kind == 'data' — cell-level TTL.
  stats <- DBI::dbGetQuery(
    handle$con,
    "SELECT COUNT(c.cell_id) AS n_present,
            MIN(c.fetched_at) AS oldest_cell
       FROM query_cells qc
       JOIN cells c ON c.cell_id = qc.cell_id
       WHERE qc.query_hash = ?;",
    params = list(query_hash)
  )

  n_expected <- as.integer(meta$n_expected[1])
  n_present  <- as.integer(stats$n_present[1] %||% 0L)

  if (n_expected > 0) {
    if (is.na(n_present) || n_present < n_expected) return(NULL)
    oldest <- stats$oldest_cell[1]
    if (is.na(oldest) || (nxt_now() - oldest) > max_age_seconds) return(NULL)
  }

  list(
    source = meta$source[1],
    entity = meta$entity[1],
    kind = "data",
    fetched_at = meta$fetched_at[1],
    template = jsonlite::fromJSON(meta$df_template[1], simplifyVector = TRUE)
  )
}

# Load all cells for a given query, returning them in the format expected by
# reconstruct_*().
#
# Performance notes:
#   * SQL fetches dim rows via a server-side JOIN, so the parameter count
#     is always 1 regardless of batch size (earlier versions tripped
#     SQLite's 999-param limit on large queries via a dynamic IN clause).
#   * We do NOT build a per-row list column for `dims`. That step was a
#     55k-iteration lapply() on large loads and dominated load time.
#     Instead, the flat `dims_lookup` tibble is attached as an attribute
#     on the returned cells tibble; `pivot_dims()` in reconstruct.R reads
#     it directly and does a vectorised pivot.
nxt_load_cells <- function(handle, query_hash) {
  rows <- DBI::dbGetQuery(
    handle$con,
    "SELECT c.cell_id, c.source, c.api_alias, c.entity_id, c.variable,
            c.period, c.dims_hash, c.value_num, c.value_txt, c.status, c.lang
       FROM cells c
       JOIN query_cells qc ON qc.cell_id = c.cell_id
       WHERE qc.query_hash = ?
       ORDER BY c.cell_id;",
    params = list(query_hash)
  )

  empty_lookup <- tibble::tibble(
    dims_hash = character(), dim_name = character(),
    dim_code  = character(), dim_label = character()
  )

  if (nrow(rows) == 0) {
    out <- tibble::as_tibble(rows)
    attr(out, "dims_lookup") <- empty_lookup
    return(out)
  }

  dims_lookup <- DBI::dbGetQuery(
    handle$con,
    "SELECT DISTINCT cd.dims_hash, cd.dim_name, cd.dim_code, cd.dim_label
       FROM cell_dims cd
       JOIN cells c        ON c.dims_hash = cd.dims_hash
       JOIN query_cells qc ON qc.cell_id = c.cell_id
      WHERE qc.query_hash = ?;",
    params = list(query_hash)
  )

  out <- tibble::as_tibble(rows)
  attr(out, "dims_lookup") <- if (nrow(dims_lookup) > 0) {
    tibble::as_tibble(dims_lookup)
  } else {
    empty_lookup
  }
  out
}
