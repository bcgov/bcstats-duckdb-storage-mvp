library(DBI)
library(odbc)
library(dplyr)
library(nanoarrow)  # For Arrow integration
library(duckdb)


# This path is retrieved from the configuration file
test_csv_folder = config::get("test_sql_server_csv")


# ---- Configuration ----
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
# ---- Connection to decimal ----
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")


# Function to copy data from CSV to MS SQL Server
copy_csv_to_mssql <- function(csv_path, mssql_conn, table_name, target_schema = my_schema, batch_size = 10000) {
  log_info <- function(msg) cat(sprintf("[%s] %s\n", Sys.time(), msg))  # Simple logging

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
    arrow_batch <- as_arrow_table(batch)

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

# Example Usage


copy_csv_to_mssql(
  csv_path = "path/to/your_file.csv",   # Path to the CSV file
  mssql_conn = decimal_con,
  table_name = "target_table",          # Target table name in MS SQL Server
  target_schema = my_schema,                # Target schema in MS SQL Server
  batch_size = 10000                    # Batch size
)

# Disconnect MS SQL Server
dbDisconnect(decimal_con)
