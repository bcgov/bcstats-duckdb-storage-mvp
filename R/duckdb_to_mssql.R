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
# install.packages("\\\\Client\\C$\\Users\\YourUserName\\Downloads\\log4r_0.4.4.tar.gz", repos = NULL, type = "source")
library(log4r)
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
# Connect to DuckDB
duckdb_conn <- dbConnect(duckdb::duckdb(), dbdir = duckdb_file)
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
# copy_table_to_mssql <- function(duckdb_conn, mssql_conn, table_name, target_schema = "Prod") {
#   # Read data from DuckDB
#   query <- paste("SELECT * FROM", table_name)
#   duckdb_data <- dbGetQueryArrow(duckdb_conn, query)
#
#   # Write data to MS SQL Server
#   dbWriteTableArrow(
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
# basicConfig(level = 'INFO') # Set logging level to INFO or DEBUG for detailed logs


library(DBI)
library(odbc)
library(dplyr)
library(nanoarrow)  # For Arrow integration
library(duckdb)

# Function to copy data from CSV to MS SQL Server
# copy_csv_to_mssql <- function(csv_path, mssql_conn, table_name, target_schema = "dbo", batch_size = 10000) {
#   log_info <- function(msg) cat(sprintf("[%s] %s\n", Sys.time(), msg))  # Simple logging
#
#   # Connect to DuckDB (in-memory)
#   duckdb_conn <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
#
#   # Create a temporary DuckDB table from the CSV
#   log_info(sprintf("Reading CSV file '%s' into DuckDB.", csv_path))
#   duckdb_table_name <- "temp_table"
#   dbExecute(duckdb_conn, sprintf("CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s')", duckdb_table_name, csv_path))
#
#   # Fetch total row count for progress tracking
#   total_rows <- dbGetQuery(duckdb_conn, sprintf("SELECT COUNT(*) AS count FROM %s", duckdb_table_name))$count
#   log_info(sprintf("CSV file '%s' contains %d rows. Starting to copy in batches.", csv_path, total_rows))
#
#   offset <- 0
#   total_rows_copied <- 0
#
#   repeat {
#     # Read a batch as an Arrow Table using dbReadTableArrow
#     query <- sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", duckdb_table_name, batch_size, offset)
#     arrow_batch <- dbGetQueryArrow(duckdb_conn, query)
#
#     # Break if no rows are left
#     if (is.null(arrow_batch)) {
#       log_info(sprintf("No more rows to copy for table '%s'. Total rows copied: %d.", table_name, total_rows_copied))
#       break
#     }
#
#     # Write the Arrow Table batch to MS SQL Server
#     dbWriteTableArrow(
#       conn = mssql_conn,
#       name = DBI::Id(schema = target_schema, table = table_name),  # Target schema and table name
#       value = arrow_batch,
#       append = (offset > 0)  # Append after the first batch
#     )
#
#     # Logging progress
#     batch_rows <- batch_size
#     total_rows_copied <- total_rows_copied + batch_rows
#     log_info(sprintf("Batch starting at offset %d for table '%s' successfully copied. Rows in batch: %d.", offset, table_name, batch_rows))
#
#     # Increment offset for the next batch
#     offset <- offset + batch_size
#   }
#
#   log_info(sprintf("Finished copying table '%s' to MS SQL Server. Total rows copied: %d.", table_name, total_rows_copied))
#
#   # Disconnect DuckDB
#   dbDisconnect(duckdb_conn)
# }

#
# # Example Usage
# mssql_conn <- dbConnect(odbc::odbc(),
#                         Driver = "ODBC Driver 17 for SQL Server",
#                         Server = "your_server",
#                         Database = "your_target_database",  # Target database
#                         UID = "your_username",
#                         PWD = "your_password",
#                         Port = 1433)
#
# copy_csv_to_mssql(
#   csv_path = "path/to/your_file.csv",   # Path to the CSV file
#   mssql_conn = mssql_conn,
#   table_name = "target_table",          # Target table name in MS SQL Server
#   target_schema = "dbo",                # Target schema in MS SQL Server
#   batch_size = 10000                    # Batch size
# )
#
# # Disconnect MS SQL Server
# dbDisconnect(mssql_conn)


# Function to copy a table using Arrow
# Advantages of dbReadTableArrow() for Data Types
# Preservation of Data Types:
#
#   Arrow Tables maintain the schema of the data as defined in the source (DuckDB in this case). This avoids implicit type coercion that can occur when data is materialized into R as a data.frame.
# Strings in DuckDB remain as strings (with leading zeros intact), and numbers retain their precision without being accidentally cast to a different type.
# Avoiding R's Type Guessing:
#
# When using collect() or dbGetQuery() to bring data into R, it relies on R's type inference, which can sometimes misinterpret string-like values as numeric (e.g., "000123" becomes 123).
# Arrow's schema-aware architecture avoids this issue by explicitly preserving the types defined in DuckDB.
# Efficient Schema Propagation:

# When writing Arrow Tables to MS SQL Server with dbWriteTableArrow(), the schema is propagated directly without intermediate transformations, ensuring consistent data types between source (DuckDB) and destination (MS SQL Server).
#
# Use nanoarrow::dbWriteTableArrow() for MS SQL Server:
#
#   The nanoarrow package provides efficient data serialization and transfer capabilities, allowing you to write directly to MS SQL Server using the Arrow format.
#
# Switching to dbReadTableArrow() ensures:
#
#   Schema integrity between DuckDB and MS SQL Server.
# Preservation of strings (e.g., no loss of leading zeros).
# Elimination of implicit type inference issues from R's data.frame.


library(DBI)
library(odbc)
library(nanoarrow)  # For Arrow integration
library(duckdb)


log_info <- function(msg, log_file = file_logger) {
  # cat(sprintf("[%s] %s\n", Sys.time(), msg))  # Simple logging
  info(log_file, msg)

}

copy_table_with_arrow <- function(duckdb_conn, mssql_conn, table_name, target_schema = "Prod") {


  # Check if the table exists in MS SQL Server and drop it if necessary
  check_and_drop_table(mssql_conn, table_name, target_schema, log_info)

  # Fetch total row count from DuckDB
  total_rows <- get_total_row_count(duckdb_conn, table_name, log_info)

  # Verify column types in DuckDB and log schema
  verify_duckdb_schema(duckdb_conn, table_name, log_info)

  # Copy data in batches
  copy_data_duckdb_mssql(duckdb_conn, mssql_conn, table_name, target_schema,  log_info)
  # or copy_data_in_batches <- function(duckdb_conn, mssql_conn, table_name, target_schema, batch_size,total_rows,log_info)

  log_info(sprintf("Finished copying table '%s' to MS SQL Server.", table_name))
}

check_and_drop_table <- function(mssql_conn, table_name, target_schema, log_info) {
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

get_total_row_count <- function(duckdb_conn, table_name, log_info) {
  total_rows_query <- sprintf("SELECT COUNT(*) AS count FROM %s", table_name)
  total_rows <- dbGetQuery(duckdb_conn, total_rows_query)$count
  log_info(sprintf("Table '%s' contains %d rows.", table_name, total_rows))
  return(total_rows)
}


verify_duckdb_schema <- function(duckdb_conn, table_name, log_info) {
  log_info(sprintf("Verifying schema for table '%s' in DuckDB.", table_name))
  schema_info <- dbGetQuery(duckdb_conn, sprintf("DESCRIBE %s", table_name))
  print(schema_info)
}

copy_data_duckdb_mssql <- function(duckdb_conn, mssql_conn, table_name, target_schema, log_info) {
  log_info(sprintf("Starting to stream data from DuckDB table '%s' to MS SQL Server.", table_name))

  # Create an array stream from DuckDB table
  stream <- dbGetQueryArrow(duckdb_conn, sprintf("SELECT * FROM %s", table_name))

  # Stream the data directly into MS SQL Server
  dbWriteTableArrow(
    conn = mssql_conn,
    name = DBI::Id(schema = target_schema, table = table_name),  # Specify schema and table name
    value = stream,  # Use the nanoarrow stream directly
    overwrite = TRUE  # Replace the table if it exists
  )

  log_info(sprintf("Completed streaming data from DuckDB table '%s' to MS SQL Server.", table_name))
}


# copy in batches, use arrow_batch.get_next() to get it stopped.
copy_data_in_batches <- function(duckdb_conn, mssql_conn, table_name, target_schema, batch_size,total_rows,log_info) {
  offset <- 0
  total_rows_copied <- 0

  repeat {
    # Read a batch as an Arrow Table using dbGetQueryArrow
    batch_query <- sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", table_name, batch_size, offset)
    arrow_batch <- dbGetQueryArrow(duckdb_conn, batch_query)

    # Check if arrow_batch is empty or NULL
    if (is.null(arrow_batch.get_next()) ) {
      log_info(sprintf("No more rows to copy for table '%s'. Total rows copied: %d.", table_name, total_rows_copied))
      break
    }

    # Write the Arrow Table batch to MS SQL Server
    dbWriteTableArrow(
      conn = mssql_conn,
      name = DBI::Id(schema = target_schema, table = table_name),  # Target schema and table name
      value = arrow_batch,
      append = (offset > 0)  # Append after the first batch
    )

    # Logging progress
    batch_rows <- batch_size  #arrow_batch$num_rows this is a nanoarrow stream and does not have the num_rows
    total_rows_copied <- if_else((total_rows_copied + batch_rows)>=total_rows, total_rows, (total_rows_copied + batch_rows))
    log_info(sprintf("Batch starting at offset %d for table '%s' successfully copied. Rows in batch: %d.", offset, table_name, batch_rows))

    # Increment offset for the next batch
    offset <- offset + batch_size
  }
}


# use it with caution.
# This function appends data from DuckDB to the existing table in MS SQL Server.
append_data_to_existing_table <- function(duckdb_conn, mssql_conn, table_name, target_schema, target_table, log_info) {
  log_info(sprintf("Starting to append data from DuckDB table '%s' to MS SQL Server table '%s.%s'.", table_name, target_schema, table_name))

  # Create an array stream from DuckDB table
  result <- dbSendQueryArrow(duckdb_conn, sprintf("SELECT * FROM %s", table_name))
  # result <- dbReadTableArrow(duckdb_conn, table_name)
  on.exit(dbClearResult(result))  # Ensure query result is cleared

  stream <- as_nanoarrow_array_stream(result)

  # Append the data directly into MS SQL Server
  dbWriteTableArrow(
    conn = mssql_conn,
    name = DBI::Id(schema = target_schema, table = target_table),  # Specify schema and table name
    value = stream,  # Use the nanoarrow stream directly
    append = TRUE  # Append to the existing table
  )

  log_info(sprintf("Completed appending data from DuckDB table '%s' to MS SQL Server table '%s.%s'.", table_name, target_schema, table_name))
}





# Query to list all tables in the DuckDB database
tables <- dbGetQuery(duckdb_conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';")

# Display the list of tables
print(tables)
# List of tables to copy
tables_to_copy <- tables$table_name[tables$table_name %>% stringr::str_detect(pattern = "stg", negate = T)]

# Loop through tables and copy them
for (table_name in tables_to_copy) {
  copy_table_with_arrow(duckdb_conn, mssql_conn= decimal_con, table_name)
}

# Disconnect from databases
dbDisconnect(duckdb_conn)
dbDisconnect(decimal_con)


