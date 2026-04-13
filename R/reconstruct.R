# Reverse of normalize_*(): given rows loaded from the `cells` + `cell_dims`
# tables plus the `template` stored at write time, rebuild the original
# source-package tibble so callers receive byte-equivalent data.

# Helper: restore the `value` column (numeric where possible, char fallback).
restore_value <- function(cells) {
  out <- cells$value_num
  txt_idx <- !is.na(cells$value_txt)
  if (any(txt_idx)) {
    out <- as.character(out)
    out[txt_idx] <- cells$value_txt[txt_idx]
  }
  out
}

# Helper: return a named list (one entry per dim_name) of per-row dimension
# codes.
#
# Two input shapes supported:
#
#   1. Fast path — `attr(cells, "dims_lookup")` is a flat tibble
#      (dims_hash, dim_name, dim_code, dim_label), produced by
#      `nxt_load_cells()`. For each `dim_col` we build a (dims_hash →
#      dim_code) lookup vector and broadcast via the cells' dims_hash in
#      a single vectorised C call. O(n) total.
#
#   2. Legacy path — `cells$dims` is a per-row list column. Used by some
#      callers that hand-build cells for tests. Falls back to a deduped
#      row iteration.
pivot_dims <- function(cells, dim_cols) {
  n <- nrow(cells)
  if (length(dim_cols) == 0 || n == 0) {
    return(setNames(
      lapply(dim_cols, function(col) rep(NA_character_, n)),
      dim_cols
    ))
  }

  dims_lookup <- attr(cells, "dims_lookup")
  if (!is.null(dims_lookup) && nrow(dims_lookup) > 0) {
    out <- vector("list", length(dim_cols))
    names(out) <- dim_cols
    for (col in dim_cols) {
      sub <- dims_lookup[dims_lookup$dim_name == col, , drop = FALSE]
      if (nrow(sub) == 0) {
        out[[col]] <- rep(NA_character_, n)
        next
      }
      lookup <- sub$dim_code
      names(lookup) <- sub$dims_hash
      vals <- unname(lookup[cells$dims_hash])
      vals[is.na(vals) | vals == NXT_NULL_SENTINEL] <- NA_character_
      out[[col]] <- vals
    }
    return(out)
  }

  # Legacy list-column path — kept for test callers that hand-build cells
  # without going through nxt_load_cells().
  unique_hashes <- unique(cells$dims_hash)
  unique_rep_idx <- match(unique_hashes, cells$dims_hash)

  unique_cols <- lapply(dim_cols, function(col) {
    vapply(unique_rep_idx, function(i) {
      d <- cells$dims[[i]]
      if (is.null(d) || length(d) == 0) return(NA_character_)
      v <- d[[col]]
      if (is.null(v) || identical(v, NXT_NULL_SENTINEL)) {
        NA_character_
      } else {
        as.character(v)
      }
    }, character(1))
  })
  names(unique_cols) <- dim_cols

  broadcast <- match(cells$dims_hash, unique_hashes)
  setNames(
    lapply(dim_cols, function(col) unique_cols[[col]][broadcast]),
    dim_cols
  )
}

reconstruct_kolada <- function(cells, template) {
  if (nrow(cells) == 0) {
    empty <- tibble::tibble(kpi = character(), year = character(),
                            value = numeric())
    return(empty)
  }

  dim_cols <- template$dim_cols %||% character(0)
  dim_vals <- pivot_dims(cells, dim_cols)

  out <- tibble::tibble(
    kpi   = cells$entity_id,
    year  = cells$period,
    value = restore_value(cells)
  )
  for (col in dim_cols) out[[col]] <- dim_vals[[col]]

  col_order <- template$columns %||% names(out)
  out <- out[, intersect(col_order, names(out)), drop = FALSE]
  tibble::as_tibble(out)
}

reconstruct_trafa <- function(cells, template) {
  if (nrow(cells) == 0) {
    return(tibble::tibble())
  }

  measures <- template$measures
  dim_cols_all <- template$dim_cols %||% character(0)

  # Group cells by their dimension vector — each group becomes one output row
  # with columns for each measure.
  # Key: paste of dim values in canonical order.
  dim_vals <- pivot_dims(cells, dim_cols_all)
  keys <- do.call(paste, c(dim_vals, list(sep = "||")))
  uniq <- !duplicated(keys)

  wide <- tibble::as_tibble(dim_vals)[uniq, , drop = FALSE]
  wide_keys <- keys[uniq]

  # One column per measure, initialized to NA
  for (m in measures) wide[[m]] <- NA_real_

  for (i in seq_len(nrow(cells))) {
    row_idx <- match(keys[i], wide_keys)
    m <- cells$variable[i]
    if (m %in% measures) {
      val <- if (!is.na(cells$value_num[i])) cells$value_num[i] else
             suppressWarnings(as.numeric(cells$value_txt[i]))
      wide[[m]][row_idx] <- val
    }
  }

  col_order <- template$columns %||% names(wide)
  wide <- wide[, intersect(col_order, names(wide)), drop = FALSE]
  tibble::as_tibble(wide)
}

reconstruct_pixieweb <- function(cells, template) {
  if (nrow(cells) == 0) return(tibble::tibble())

  dim_cols_all <- template$dim_cols %||% character(0)
  dim_vals <- pivot_dims(cells, dim_cols_all)

  out <- tibble::tibble(table_id = cells$entity_id)
  for (col in dim_cols_all) out[[col]] <- dim_vals[[col]]

  if (!is.na(template$time_col)) {
    out[[template$time_col]] <- ifelse(cells$period == NXT_NULL_SENTINEL,
                                       NA_character_, cells$period)
  }

  if (isTRUE(template$has_contents)) {
    out$ContentsCode <- ifelse(cells$variable == NXT_NULL_SENTINEL,
                               NA_character_, cells$variable)
  }

  out$value <- restore_value(cells)

  col_order <- template$columns %||% names(out)
  out <- out[, intersect(col_order, names(out)), drop = FALSE]
  tibble::as_tibble(out)
}

# Dispatcher for the generic cache_handler path.
reconstruct_source <- function(source, cells, template) {
  switch(source,
    kolada   = reconstruct_kolada(cells, template),
    trafa    = reconstruct_trafa(cells, template),
    pixieweb = reconstruct_pixieweb(cells, template),
    abort(sprintf("reconstruct_source: unknown source '%s'", source))
  )
}
