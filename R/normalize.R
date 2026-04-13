# Convert source-package tibbles into the cell-level format used by the
# nordstatExtras backend.
#
# Each normalizer returns a list with two tibbles:
#   $cells — rows ready to UPSERT into `cells`, plus a `dims` list column
#            carrying the named dimension vector for each cell.
#   $template — a JSON-encodable list describing the original df shape so
#            `reconstruct_*()` can restore it byte-for-byte on load.

# Core constructor — build the gatekeeper-format tibble from per-cell fields.
#
# `dims_hash` is optional: when provided (e.g. from a vectorised call to
# `vec_build_dims()`), we skip the per-row `hash_dims()` loop. For large
# batches that would otherwise call `digest::digest()` once per row, this
# can be orders of magnitude faster — many rows share the same dimension
# vector, and the caller dedupes before hashing.
make_cells <- function(source, entity_id,
                       variable  = NXT_NULL_SENTINEL,
                       period    = NXT_NULL_SENTINEL,
                       dims      = list(),
                       dims_hash = NULL,
                       value,
                       status    = NA_character_,
                       lang      = NXT_NULL_SENTINEL,
                       api_alias = NXT_NULL_SENTINEL) {
  n <- length(value)
  stopifnot(length(dims) == n || length(dims) == 0)

  if (is.null(dims_hash)) {
    dims_hash <- if (length(dims) == 0) {
      rep(hash_dims(list()), n)
    } else {
      vapply(dims, hash_dims, character(1))
    }
  }

  value_num <- suppressWarnings(as.numeric(value))
  value_txt <- ifelse(is.na(value_num), as.character(value), NA_character_)

  tibble::tibble(
    source    = source,
    api_alias = coerce_sentinel(api_alias),
    entity_id = as.character(entity_id),
    variable  = coerce_sentinel(variable),
    period    = coerce_sentinel(period),
    dims_hash = dims_hash,
    value_num = value_num,
    value_txt = value_txt,
    status    = as.character(status),
    lang      = coerce_sentinel(lang),
    dims      = dims
  )
}

# Build a dimensions list column + a dims_hash vector from a data frame
# and a set of dimension column names, column-wise.
#
# Previous implementation used `df[i, dim_cols, drop = FALSE]` per row —
# slow in base R and catastrophic in tibble. This version:
#
#   1. Pre-extracts each dimension column once as a character vector (with
#      NA → sentinel applied in one vectorised pass).
#   2. Builds a single blob string per row via vectorised paste, then
#      deduplicates before hashing — most batches have many fewer unique
#      dimension vectors than rows.
#   3. Constructs the per-row list column by vector subscripting into the
#      pre-extracted columns (no data frame indexing).
#
# Returns a list with `dims_list` (the per-row named list) and `dims_hash`
# (a character vector parallel to `df`'s rows).
vec_build_dims <- function(df, dim_cols) {
  n <- nrow(df)
  if (n == 0 || length(dim_cols) == 0) {
    return(list(
      dims_hash = if (n > 0) rep(hash_dims(list()), n) else character(0),
      dims_list = vector("list", n)
    ))
  }

  col_vals <- lapply(dim_cols, function(nm) {
    v <- as.character(df[[nm]])
    v[is.na(v)] <- NXT_NULL_SENTINEL
    v
  })
  names(col_vals) <- dim_cols

  # Hash inputs must be sorted by dim name for determinism — mirrors
  # `hash_dims()`'s behavior on a per-row list.
  sorted_cols <- sort(dim_cols)
  blob_parts <- lapply(sorted_cols, function(nm) {
    paste0(nm, "=", col_vals[[nm]])
  })
  blobs <- do.call(paste, c(blob_parts, list(sep = ";")))

  # Dedupe blobs before hashing — a 100k-row batch with 1500 unique dim
  # combinations does 1500 digest calls instead of 100k.
  unique_blobs <- unique(blobs)
  unique_hashes <- vapply(unique_blobs, function(b) {
    substr(digest::digest(b, algo = "sha1"), 1, 16)
  }, character(1))
  dims_hash <- unique_hashes[match(blobs, unique_blobs)]

  # Build the per-row dims list via fast vector subscripting.
  dims_list <- vector("list", n)
  template_names <- dim_cols
  for (i in seq_len(n)) {
    row <- vector("list", length(dim_cols))
    names(row) <- template_names
    for (j in seq_along(dim_cols)) {
      row[[j]] <- col_vals[[j]][i]
    }
    dims_list[[i]] <- row
  }

  list(dims_hash = dims_hash, dims_list = dims_list)
}

# ---- rKolada ---------------------------------------------------------------
#
# get_values() returns a tibble with:
#   kpi, municipality_id, municipality, municipality_type, year, value, gender
#
# Universal mapping:
#   entity_id ← kpi
#   period    ← year
#   value     ← value
#   dims      ← list(municipality_id, municipality, municipality_type, gender)
#
# Anything else (e.g. status flags in non-simplified mode) goes into dims.
normalize_kolada <- function(df, lang = "sv") {
  stopifnot(tibble::is_tibble(df) || is.data.frame(df))
  if (nrow(df) == 0) {
    return(list(
      cells = make_cells("kolada", character(0), value = numeric(0)),
      template = list(source = "kolada", columns = names(df),
                      lang = lang)
    ))
  }

  required <- c("kpi", "year", "value")
  missing <- setdiff(required, names(df))
  if (length(missing)) {
    abort(sprintf("normalize_kolada: missing required columns: %s",
                  paste(missing, collapse = ", ")))
  }

  dim_cols <- setdiff(names(df), c("kpi", "year", "value"))

  dims_info <- vec_build_dims(df, dim_cols)

  cells <- make_cells(
    source    = "kolada",
    entity_id = df$kpi,
    period    = df$year,
    dims      = dims_info$dims_list,
    dims_hash = dims_info$dims_hash,
    value     = df$value,
    lang      = lang
  )

  list(
    cells = cells,
    template = list(
      source = "kolada",
      columns = names(df),
      dim_cols = dim_cols,
      lang = lang
    )
  )
}

# ---- rTrafa ----------------------------------------------------------------
#
# get_data() returns a tibble where:
#   - dimension columns are named after the dimension (e.g. `ar`, `drivm`)
#   - when simplify=TRUE, each dimension has a sibling `{name}_label`
#   - one or more measure columns carry numeric values (e.g. `itrfslut`)
#
# Universal mapping (one cell per (row × measure)):
#   entity_id ← product     (supplied via `product` argument — not in df)
#   variable  ← measure name
#   period    ← `ar` column if present, otherwise sentinel
#   value     ← the measure column value
#   dims      ← all non-measure, non-period columns (codes + labels)
normalize_trafa <- function(df, product, measures = NULL, lang = "SV") {
  stopifnot(tibble::is_tibble(df) || is.data.frame(df))
  if (nrow(df) == 0) {
    return(list(
      cells = make_cells("trafa", character(0), value = numeric(0)),
      template = list(source = "trafa", columns = names(df),
                      product = product, measures = measures, lang = lang)
    ))
  }

  cols <- names(df)
  label_cols <- grep("_label$", cols, value = TRUE)
  dim_code_cols <- sub("_label$", "", label_cols)
  # Measures = numeric columns that are not dimension codes / labels
  non_meta <- setdiff(cols, c(label_cols, dim_code_cols))
  if (is.null(measures)) {
    measures <- non_meta[vapply(df[non_meta], is.numeric, logical(1))]
  }
  if (length(measures) == 0) {
    abort("normalize_trafa: could not identify any measure columns.")
  }

  dim_cols_all <- setdiff(cols, measures)
  period_col <- if ("ar" %in% dim_code_cols) "ar" else NA_character_

  # Hoist the dim construction out of the per-measure loop — the dim
  # vectors are identical across measures (only `value` / `variable`
  # change), so there's no point re-building them three times.
  dims_info <- vec_build_dims(df, dim_cols_all)
  period_vec <- if (!is.na(period_col)) {
    as.character(df[[period_col]])
  } else {
    NXT_NULL_SENTINEL
  }

  parts <- lapply(measures, function(m) {
    make_cells(
      source    = "trafa",
      entity_id = product,
      variable  = m,
      period    = period_vec,
      dims      = dims_info$dims_list,
      dims_hash = dims_info$dims_hash,
      value     = df[[m]],
      lang      = lang
    )
  })

  cells <- do.call(rbind, parts)

  list(
    cells = cells,
    template = list(
      source = "trafa",
      columns = cols,
      product = product,
      measures = measures,
      dim_cols = dim_cols_all,
      period_col = period_col,
      lang = lang
    )
  )
}

# ---- pixieweb --------------------------------------------------------------
#
# get_data() in long mode returns:
#   table_id, <dim_code>, <dim_code>_text, ..., [ContentsCode, ContentsCode_text,] value
#
# Universal mapping:
#   entity_id ← table_id
#   api_alias ← alias (supplied arg)
#   variable  ← ContentsCode if present, else sentinel
#   period    ← Tid column (or time-dim code) if detectable, else sentinel
#   value     ← value column
#   dims      ← all dim columns except the one mapped to period
normalize_pixieweb <- function(df, alias = NXT_NULL_SENTINEL, lang = "sv",
                               time_col = NULL) {
  stopifnot(tibble::is_tibble(df) || is.data.frame(df))
  if (nrow(df) == 0) {
    return(list(
      cells = make_cells("pixieweb", character(0), value = numeric(0)),
      template = list(source = "pixieweb", columns = names(df),
                      alias = alias, lang = lang, time_col = time_col)
    ))
  }

  cols <- names(df)
  if (!"value" %in% cols) {
    abort("normalize_pixieweb: df must contain a `value` column.")
  }
  if (!"table_id" %in% cols) {
    abort("normalize_pixieweb: df must contain a `table_id` column.")
  }

  # Heuristic: time column is "Tid" if present, else first column whose name
  # matches /^(tid|time|year|ar)$/i.
  if (is.null(time_col)) {
    candidates <- grep("^(tid|time|year|ar)$", cols, ignore.case = TRUE, value = TRUE)
    time_col <- if (length(candidates) > 0) candidates[1] else NA_character_
  }

  has_contents <- "ContentsCode" %in% cols
  variable_vec <- if (has_contents) as.character(df$ContentsCode) else NXT_NULL_SENTINEL

  non_dim <- c("table_id", "value", if (has_contents) c("ContentsCode", "ContentsCode_text"))
  period_vec <- if (!is.na(time_col)) as.character(df[[time_col]]) else NXT_NULL_SENTINEL
  dim_cols_all <- setdiff(cols, c(non_dim, if (!is.na(time_col)) time_col))

  entity_ids <- as.character(df$table_id)

  dims_info <- vec_build_dims(df, dim_cols_all)

  cells <- make_cells(
    source    = "pixieweb",
    entity_id = entity_ids,
    variable  = variable_vec,
    period    = period_vec,
    dims      = dims_info$dims_list,
    dims_hash = dims_info$dims_hash,
    value     = df$value,
    lang      = lang,
    api_alias = alias
  )

  list(
    cells = cells,
    template = list(
      source = "pixieweb",
      columns = cols,
      alias = alias,
      lang = lang,
      time_col = time_col,
      has_contents = has_contents,
      dim_cols = dim_cols_all
    )
  )
}
