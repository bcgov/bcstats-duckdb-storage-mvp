# Copyright 2024 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

get_csvs = function(paths, df) {

  paths_df = params$paths |>
    enframe() |>
    set_names(c("source", "dir")) |>

    # we call this a 'kluge!!!!!!!!!!!!!'
    mutate(dir = case_match(dir,
      "G:/Operations/Data Science and Analytics/2024_bcstats_db" ~ "G:/Operations/Data Science and Analytics/2024_bcstats_db/csvs",
      "C:/Users/thister/OneDrive - Government of BC/2024-025 Brett and Jon Database Test Warehouse" ~ "C:/Users/thister/OneDrive - Government of BC/2024-025 Brett and Jon Database Test Warehouse/raw_data"))

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

make_query = function(prefix, base, suffix) eval(parse(text = paste(prefix, base, suffix, sep = " |> ")))


read_csv_target = function(path, base_query) {
  # read the dataframe into `df`
  start = Sys.time()
  df = readr::read_csv(path, quote = "\"", progress = F, show_col_types = F)
  end = Sys.time()
  read_time = difftime(end, start)

  # run the queries on df
  query_time = apply(filter(params$query_additions, !use_db), 1, function(row) {
    query = make_query(row[['prefix']], base_query, row[['suffix']])
    start = Sys.time()
    query(df)
    end = Sys.time()
    difftime(end, start)
  })

  return(list(read_time = read_time, query_time = query_time))
}




duckDB_target = function(dbdir, path, base_query) {

  # just in case the db already exists
  if (fs::file_exists(dbdir)) {
    db = duckdb::dbConnect(duckdb::duckdb(), dbdir = dbdir)
    duckdb::dbDisconnect(db, shutdown = TRUE)
    gc()
    try(fs::file_delete(dbdir))
  }

  # attempt to create the duckDB db
  db = duckdb::dbConnect(duckdb::duckdb(), dbdir = dbdir)
  start = Sys.time()
  success = T
  success = tryCatch({
    duckdb::duckdb_read_csv(conn = db, name = 't1', files = path, header = T, nrow.check = Inf, transaction = T)
  }, error = function(e) return(FALSE))
  end = Sys.time()

  if (!success) success = TRUE # kluge?

  # sometimes it doesn't work, so re-run but use `nrow.check = Inf`
  if (!success) {

    # remove the db from previous step
    if (fs::file_exists(dbdir)) {
      gc()
      try(fs::file_delete(dbdir))
    }

    # read the db with `nrow.check = Inf`
    db = duckdb::dbConnect(duckdb::duckdb(), dbdir = dbdir)
    start = Sys.time()
    duckdb::duckdb_read_csv(conn = db, name = 't1', files = path, header = T, nrow.check = Inf, transaction = T)
    end = Sys.time()
  }
  read_time = difftime(end, start)

  # run the queries on the duckdb db
  query_time = apply(filter(params$query_additions, use_db), 1, function(row) {
    query = make_query(row[['prefix']], base_query, row[['suffix']])
    start = Sys.time()
    query(db)
    end = Sys.time()
    difftime(end, start)
  })

  duckdb::dbDisconnect(db, shutdown = TRUE)
  gc()
  try(fs::file_delete(dbdir))

  return(list(success = success, read_time = read_time, query_time = query_time))
}
