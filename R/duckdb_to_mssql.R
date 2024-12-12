# Workflow in R
# Load Data into DuckDB Using dbt
#
# This step remains the same as in the Python approach. You use dbt to run your transformations and load data into DuckDB.
# Copy Data from DuckDB to MS SQL Server in R
#
# R can read tables from DuckDB and write them into MS SQL Server using the DBI and odbc packages.

library(DBI)
library(odbc)
library(duckdb)
library(dplyr)
library(logging)
library(arrow)
library(nanoarrow)  # For Arrow integration
# File paths and connection details
# Define the path to the test CSV folder
# This path is retrieved from the configuration file
test_csv_folder = config::get("test_sql_server_csv")


# Define the folder path
db_folder_path <- file.path(test_csv_folder, "db")

dev_db_path <- file.path(db_folder_path, "bcstats_db_prod.duckdb")

dev_db_con <- duckdb::dbConnect(duckdb::duckdb(), dbdir = dev_db_path)

# Query to list all tables in the DuckDB database
tables <- dbGetQuery(dev_db_con, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';")

# Display the list of tables
print(tables)

duckdb_file <- file.path(db_folder_path, "bcstats_db_prod.duckdb")
# mssql_conn <- dbConnect(odbc::odbc(),
#                         Driver = "ODBC Driver 17 for SQL Server",
#                         Server = "your_server",
#                         Database = "your_database",
#                         UID = "your_username",
#                         PWD = "your_password",
#                         Port = 1433)
# ---- Configuration ----
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
# ---- Connection to decimal ----
decimal_con <- dbConnect(odbc::odbc(),
                         Driver = db_config$driver,
                         Server = db_config$server,
                         Database = db_config$database,
                         Trusted_Connection = "True")

# Function to copy a table from DuckDB to MS SQL Server
# copy_table_to_mssql <- function(duckdb_conn, mssql_conn, table_name, target_schema = "dbo") {
#   # Read data from DuckDB
#   query <- paste("SELECT * FROM", table_name)
#   duckdb_data <- dbGetQuery(duckdb_conn, query)
#
#   # Write data to MS SQL Server
#   dbWriteTable(
#     conn = mssql_conn,
#     name = DBI::Id(schema = target_schema, table = table_name),
#     value = duckdb_data,
#     overwrite = TRUE,  # Replace the table if it exists
#     row.names = FALSE
#   )
#
#   cat(sprintf("Table '%s' copied to MS SQL Server successfully.\n", table_name))
# }


# Batching Large Tables: If tables are very large, you can implement batching by reading and writing data in chunks.

# Initialize logger
basicConfig(level = 'INFO') # Set logging level to INFO or DEBUG for detailed logs

copy_table_to_mssql <- function(duckdb_conn, mssql_conn, table_name, target_schema = "prod", batch_size = 100000) {
  offset <- 0
  total_rows_copied <- 0
  loginfo(sprintf("Starting to copy table '%s' from DuckDB to MS SQL Server.", table_name))

  repeat {
    tryCatch({
      # Fetch batch from DuckDB
      query <- sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", table_name, batch_size, offset)
      duckdb_data <- dbGetQuery(duckdb_conn, query)

      if (nrow(duckdb_data) == 0) {
        loginfo(sprintf("No more rows to copy for table '%s'. Total rows copied: %d.", table_name, total_rows_copied))
        break # Exit loop if no more rows
      }

      # Write batch to MS SQL Server
      dbWriteTable(
        conn = mssql_conn,
        name = DBI::Id(schema = target_schema, table = table_name),
        value = duckdb_data,
        append = (offset > 0), # Append after the first batch
        row.names = FALSE
      )

      # Log progress
      batch_rows <- nrow(duckdb_data)
      total_rows_copied <- total_rows_copied + batch_rows
      loginfo(sprintf("Batch starting at offset %d for table '%s' successfully copied. Rows in batch: %d.", offset, table_name, batch_rows))

      # Increment offset
      offset <- offset + batch_size
    }, error = function(e) {
      logerror(sprintf("Error while processing table '%s' at offset %d: %s", table_name, offset, e$message))
      stop(e) # Stop the process for critical errors
    })
  }

  loginfo(sprintf("Finished copying table '%s' to MS SQL Server. Total rows copied: %d.", table_name, total_rows_copied))
}


# Function to copy a table using Arrow
# Use tbl() for Lazy Evaluation:
#
#   With tbl(), you get a handle to the DuckDB table without loading it into memory. This allows you to work with the data in chunks.
# Use nanoarrow::dbWriteTableArrow() for MS SQL Server:
#
#   The nanoarrow package provides efficient data serialization and transfer capabilities, allowing you to write directly to MS SQL Server using the Arrow format.

copy_table_with_arrow <- function(duckdb_conn, mssql_conn, table_name, target_schema = "prod", batch_size = 100000) {
  log_info <- function(msg) cat(sprintf("[%s] %s\n", Sys.time(), msg))  # Simple logging

  # Get a handle to the DuckDB table using tbl()
  duckdb_table <- tbl(duckdb_conn, table_name)

  # Fetch total row count for progress tracking
  total_rows <- duckdb_table %>% summarise(count = n()) %>% collect() %>% pull(count)
  log_info(sprintf("Starting to copy table '%s' with %d rows in batches.", table_name, total_rows))

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
    batch_rows <- nrow(as.data.frame(arrow_batch))
    total_rows_copied <- total_rows_copied + batch_rows
    log_info(sprintf("Batch starting at offset %d for table '%s' successfully copied. Rows in batch: %d.", offset, table_name, batch_rows))

    # Increment offset for the next batch
    offset <- offset + batch_size
  }

  log_info(sprintf("Finished copying table '%s' to MS SQL Server. Total rows copied: %d.", table_name, total_rows_copied))
}


# Connect to DuckDB
duckdb_conn <- dbConnect(duckdb::duckdb(), dbdir = duckdb_file)
# Query to list all tables in the DuckDB database
tables <- dbGetQuery(duckdb_conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';")

# Display the list of tables
print(tables)
# List of tables to copy
tables_to_copy <- tables$table_name[tables$table_name %>% stringr::str_detect(pattern = "stg", negate = T)]

# Loop through tables and copy them
for (table_name in tables_to_copy) {
  copy_table_with_arrow(duckdb_conn, mssql_conn= decimal_con, table_name,batch_size = 1000000)
}

# Disconnect from databases
dbDisconnect(duckdb_conn)
dbDisconnect(mssql_conn)
