get_csvs = function(paths_df, df) {
  fs::dir_info(paths_df$dir, regexp = "\\.csv$") |>
    mutate(dir = fs::path_dir(path)) |>
    mutate(csv = fs::path_file(path), .before=1) |>
    inner_join(paths_df) |>
    inner_join(df |> mutate(csv_id = row_number())) |>
    mutate(csv = factor(csv, levels = df$csv)) |>
    arrange(csv) |>
    mutate(csv = as.character(csv)) |>
    mutate(base_query = dplyr::sql(base_query)) |>
    select(source, csv_id, csv, path, size, base_query) |>
    arrange(source, csv_id)
}

read_data_with_duckDB = function(file, name = "t1", header = T, transaction = T, nrow.check = 500) {
  tryCatch({
    duckdb::duckdb_read_csv(conn = db, name = name, files = file, header = header, nrow.check = nrow.check, transaction = transaction)
    return(TRUE)
  }, error = function(e) {
    return(FALSE)
  })
}

make_query = function(prefix, base, suffix) eval(parse(text = paste(prefix, base, suffix, sep = " |> ")))
