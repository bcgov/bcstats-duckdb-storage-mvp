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




# Function to copy data from CSV to MS SQL Server
copy_csv_to_mssql <- function(csv_path, mssql_conn, table_name, target_schema = "dev", batch_size = 100000) {

  # Connect to DuckDB (in-memory)
  duckdb_conn <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

  # Read the CSV using DuckDB
  log_info(sprintf("Reading CSV file '%s' into DuckDB.", csv_path))
  duckdb_table <- tbl(duckdb_conn, paste0("read_csv_auto('", csv_path, "')"))  # Lazily load CSV

  # Fetch total row count for progress tracking
  total_rows <- duckdb_table %>% summarise(count = n()) %>% collect() %>% pull(count)
  log_info(sprintf("CSV file '%s' contains %d rows. Starting to copy in batches.", csv_path, total_rows))

  offset <- 0
  total_rows_copied <- 0

  repeat {
    # Select a batch using filter() with row_number()
    batch <- duckdb_table %>%
      filter(row_number() > offset & row_number() <= (offset + batch_size)) %>%
      collect()  # Materialize the batch in memory

    # Break if no rows are left
    if (nrow(batch) == 0) {
      log_info(sprintf("No more rows to copy for table '%s'. Total rows copied: %d.", table_name, total_rows_copied))
      break
    }

    # Convert the batch to an Arrow Table
    arrow_batch <- arrow::as_arrow_table(batch)

    # Write the batch to MS SQL Server using nanoarrow
    dbWriteTableArrow(
      conn = mssql_conn,
      name = DBI::Id(schema = target_schema, table = table_name),  # Target schema and table name
      value = arrow_batch,
      append = (offset > 0)  # Append after the first batch
    )

    # Logging progress
    batch_rows <- nrow(batch)
    total_rows_copied <- total_rows_copied + batch_rows
    log_info(sprintf("Batch starting at offset %d for table '%s' successfully copied. Rows in batch: %d.", offset, table_name, batch_rows))

    # Increment offset for the next batch
    offset <- offset + batch_size
  }

  log_info(sprintf("Finished copying table '%s' to MS SQL Server. Total rows copied: %d.", table_name, total_rows_copied))

  # Disconnect DuckDB
  dbDisconnect(duckdb_conn)
}




log_info <- function(msg, log_file = file_logger) {
  cat(sprintf("[%s] %s\n", Sys.time(), msg))  # Simple logging
  info(log_file, msg)

}

create_csv_tbl_duckdb <- function(duckdb_conn, csv_file_path, table_name){

}


check_and_drop_table <- function(mssql_conn, table_name, target_schema) {
  check_table_query <- sprintf("
    SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", target_schema, table_name)

  table_exists <- dbGetQuery(mssql_conn, check_table_query)

  if (nrow(table_exists) > 0) {
    log_info(sprintf("Table '%s.%s' exists in MS SQL Server. Dropping the table.", target_schema, table_name))
    dbExecute(mssql_conn, sprintf("DROP TABLE [%s].[%s]", target_schema, table_name))
  } else {
    log_info(sprintf("Table '%s.%s' does not exist in MS SQL Server. Proceeding with the copy.", target_schema, table_name))
  }
}

get_total_row_count <- function(duckdb_conn, table_name) {
  total_rows_query <- sprintf("SELECT COUNT(*) AS count FROM %s", table_name)
  total_rows <- dbGetQuery(duckdb_conn, total_rows_query)$count
  log_info(sprintf("Table '%s' contains %d rows.", table_name, total_rows))
  return(total_rows)
}


verify_duckdb_schema <- function(duckdb_conn, table_name) {
  log_info(sprintf("Verifying schema for table '%s' in DuckDB.", table_name))
  schema_info <- dbGetQuery(duckdb_conn, sprintf("DESCRIBE %s", table_name))
  print(schema_info)
}

copy_data_duckdb_mssql <- function(duckdb_conn, mssql_conn, table_name, target_schema) {
  log_info(sprintf("Starting to stream data from DuckDB table '%s' to MS SQL Server.", table_name))

  # Create an array stream from DuckDB table
  result <- dbGetQueryArrow(duckdb_conn, sprintf("SELECT * FROM %s", table_name))

  stream <- as_nanoarrow_array_stream(result)

  # Stream the data directly into MS SQL Server
  dbWriteTableArrow(
    conn = mssql_conn,
    name = DBI::Id(schema = target_schema, table = table_name),  # Specify schema and table name
    value = stream  # Use the nanoarrow stream directly
    # overwrite = TRUE  # Replace the table if it exists
  )

  log_info(sprintf("Completed streaming data from DuckDB table '%s' to MS SQL Server.", table_name))
}


# copy in batches, use arrow_batch.get_next() to get it stopped.
copy_data_duckdb_mssql_in_batches <- function(duckdb_conn, mssql_conn, table_name, target_schema, batch_size,total_rows) {
  offset <- 0
  total_rows_copied <- 0

  repeat {
    # Read a batch as an Arrow Table using dbGetQueryArrow
    batch_query <- sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", table_name, batch_size, offset)
    arrow_batch <- dbGetQueryArrow(duckdb_conn, batch_query)
    # arrow_batch <- dbSendQueryArrow(duckdb_conn, batch_query)
    # on.exit(dbClearResult(arrow_batch))  # Ensure query result is cleared


    batch_rows <- dbGetQuery(duckdb_conn, sprintf("SELECT count(*) as count from (SELECT * FROM %s LIMIT %d OFFSET %d)", table_name, batch_size, offset))$count

    # Check if arrow_batch is empty or NULL
    if (is.null(arrow_batch) || batch_rows == 0 ) {
      log_info(sprintf("No more rows to copy for table '%s'. Total rows copied: %d.", table_name, total_rows_copied))
      break
    }

    log_info(sprintf("Batch starting at offset %d for table '%s' successfully copied. Rows in batch: %d.", offset, table_name, batch_rows))
    # arrow_stream = arrow_batch%>% as_nanoarrow_array_stream()
    # Write the Arrow Table batch to MS SQL Server
    dbWriteTableArrow(
      conn = mssql_conn,
      name = DBI::Id(schema = target_schema, table = table_name),  # Target schema and table name
      value = arrow_batch ,
      append = (offset > 0)  # Append after the first batch
    )

    # Logging progress
    total_rows_copied <- if_else((total_rows_copied + batch_rows)>=total_rows, total_rows, (total_rows_copied + batch_rows))


    log_info(glue::glue ("Total {total_rows_copied} rows of total {total_rows} rows copied."))
    if (total_rows_copied>= total_rows) break
    # Increment offset for the next batch
    offset <- offset + batch_size
  }
}


update_progress_bar <- function(total_rows_copied, total_rows, progress_bar, last_logged_percentage) {
  # Calculate percentage completed
  percentage_completed <- floor((total_rows_copied / total_rows) * 100)

  # If 1% or more progress has been made, update the progress bar
  if (percentage_completed > last_logged_percentage) {
    new_progress <- strrep("-", percentage_completed - last_logged_percentage)
    progress_bar <- paste0(progress_bar, new_progress)

    # Print updated progress bar and percentage
    cat(sprintf("\r[%s] %d%% (%d/%d rows copied)", progress_bar, percentage_completed, total_rows_copied, total_rows))
    flush.console()  # Ensure immediate printing to console
    log_info(glue::glue ("Total {total_rows_copied} rows of total {total_rows} rows copied."))
  }

  return(list(progress_bar = progress_bar, last_logged_percentage = percentage_completed))
}


# all the funcitons in csv to mssql already implemented in R dbi functions, so we can refactor them by replacing our functions with dbi functions.

# copy in batches, use arrow_batch.get_next() to get it stopped.
copy_data_duckdb_mssql_in_chunk <- function(duckdb_conn, mssql_conn, table_name, target_schema, total_rows, batch_size = 256*100) {
  # Initialize progress bar and copied row count
  total_rows_copied <- 0
  last_logged_percentage <- 0  # Tracks the last percentage logged
  progress_bar <- ""           # String to represent the progress bar

    # Read a batch as an Arrow Table using dbGetQueryArrow
    table_query <- sprintf("SELECT * FROM %s", table_name)
    arrow_query_rs <- dbSendQueryArrow(duckdb_conn, table_query)

    # print(dbColumnInfo(arrow_query_rs))

    while(TRUE) {
    # Check if arrow_batch is empty or NULL
    if (dbHasCompleted(arrow_query_rs) ) {
      log_info(sprintf("No more rows to copy for table '%s'. Total rows copied: %d.", table_name, total_rows_copied))
      break
    }

    arrow_chunk <-  dbFetchArrowChunk(arrow_query_rs)

    # Write the Arrow Table batch to MS SQL Server
    dbWriteTableArrow(
      conn = mssql_conn,
      name = DBI::Id(schema = target_schema, table = table_name),  # Target schema and table name
      value = arrow_chunk ,
      append = total_rows_copied > 0 # Append after the first batch
    )

    # Update total rows copied
    total_rows_copied <- dbGetRowCount(arrow_query_rs)

    # Update progress bar and log every 1% completion
    progress_info <- update_progress_bar(total_rows_copied, total_rows, progress_bar, last_logged_percentage)
    progress_bar <- progress_info$progress_bar
    last_logged_percentage <- progress_info$last_logged_percentage

    # Exit loop if all rows are copied
    if (total_rows_copied>= total_rows) break
    }
    on.exit(dbClearResult(arrow_query_rs))  # Ensure query result is cleared
}





# Function to copy data from CSV to MS SQL Server
copy_duckdb_csv_to_mssql <- function(csv_path, mssql_conn, table_name, target_schema = "dev", batch_size = 100000) {
  log_info(sprintf("Start reading CSV file '%s'.", csv_path))
  # Connect to DuckDB (in-memory)
  duckdb_conn <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

  # Read the CSV using DuckDB
  log_info(sprintf("Reading CSV file '%s' into DuckDB.", csv_path))
  # duckdb_table <- tbl(duckdb_conn, paste0("read_csv_auto('", csv_path, "')"))  # Lazily load CSV

  test_csv_number = duckdb_read_csv(conn = duckdb_conn,
                                    name = table_name,
                                    files = csv_path
  )

  # str(duckdb_table)
  # Fetch total row count from DuckDB
  total_rows <- get_total_row_count(duckdb_conn, table_name)


  # Check if the table exists in MS SQL Server and drop it if necessary
  check_and_drop_table(mssql_conn, table_name, target_schema)

  # Verify column types in DuckDB and log schema
  verify_duckdb_schema(duckdb_conn, table_name)

  log_info(sprintf("Started copying table '%s' to MS SQL Server.", table_name))
  # copy_data_duckdb_mssql(duckdb_conn, mssql_conn, table_name, target_schema)
  # copy_data_duckdb_mssql_in_batches(duckdb_conn, mssql_conn, table_name, target_schema, batch_size,total_rows)
  # copy_csv_to_mssql(csv_path, mssql_conn, table_name, target_schema, batch_size,total_rows)
  copy_data_duckdb_mssql_in_chunk(duckdb_conn, mssql_conn, table_name, target_schema, total_rows)

  log_info(sprintf("Finished copying table '%s' to MS SQL Server.", table_name))

  dbDisconnect(duckdb_conn)
}


