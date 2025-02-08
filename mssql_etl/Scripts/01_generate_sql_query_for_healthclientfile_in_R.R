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

# Copyright 2025 Province of British Columbia
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


############################################################################################################################################
# Dev database: duckdb, Prod database: MS SQL server
############################################################################################################################################


# Load required library
library(tidyverse)
library(fs)
library(stringr)
library(DBI)
library(odbc)
library(dplyr)
library(arrow)
# install.packages("\\\\Client\\C$\\Users\\YourUserName\\Downloads\\nanoarrow_0.6.0.tar.gz", repos = NULL, type = "source")
library(nanoarrow)  # For Arrow integration
library(duckdb)
library(log4r)
source("./mssql_etl/Scripts/functions.r")

# This path is retrieved from the configuration file
lan_csv_file_path = config::get("lan_csv_file_path")

log_dir= "DATABASE/Citrix/log/"

dir.create(file.path(lan_csv_file_path,log_dir))

log_file_path = file.path(file.path(lan_csv_file_path,log_dir), glue::glue("Read_csv_file_write_to_sqlserver_{Sys.Date()}.log"))

file_logger = logger(appenders = file_appender(log_file_path))

info(file_logger, "Starte reading csv file write to sqlserver")

# ---- Configuration ----
# prod database
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
# ---- Connection to decimal ----
decimal_conn <- dbConnect(odbc::odbc(),
                          Driver = db_config$driver,
                          Server = db_config$server,
                          Database = db_config$database,
                          Trusted_Connection = "True")

# Query to list all tables in the DuckDB database
dev_tables <- dbGetQuery(decimal_conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'dev';")

# Display the list of tables
print(dev_tables)

health_client_tables <- dev_tables %>% filter(str_detect(table_name, pattern = "CLR_EXT_|bc_stat_population_estimates|BC_STAT_POPULATION_ESTIMATES"))
health_client_tables$table_name %>% paste(  collapse = "', '")

# remove   'CLR_EXT_20200213_for_201107', now.
table_names = c(
  'CLR_EXT_20160927',
  'CLR_EXT_20161027',
  'CLR_EXT_20161128',
  'CLR_EXT_20161229',
  'CLR_EXT_20170126',
  'CLR_EXT_20170227',
  'CLR_EXT_20170324',
  'CLR_EXT_20170427',
  'CLR_EXT_20170626',
  'CLR_EXT_20170727',
  'CLR_EXT_20170825',
  'CLR_EXT_20170928',
  'CLR_EXT_20171026',
  'CLR_EXT_20171128',
  'CLR_EXT_20171228',
  'CLR_EXT_20180228',
  'CLR_EXT_20180327',
  'CLR_EXT_20180426',
  'CLR_EXT_20180529',
  'CLR_EXT_20180628',
  'CLR_EXT_20180829',
  'CLR_EXT_20181129',
  'CLR_EXT_20181224',
  'CLR_EXT_20190128',
  'CLR_EXT_20190228',
  'CLR_EXT_20190327',
  'CLR_EXT_20190429',
  'CLR_EXT_20190628',
  'CLR_EXT_20190729',
  'CLR_EXT_20190829',
  'CLR_EXT_20180730',
  'CLR_EXT_20190927',
  'CLR_EXT_20191028',
  'CLR_EXT_20191128',
  'CLR_EXT_20191224',
  'CLR_EXT_20200127',
  'CLR_EXT_20200227',
  'CLR_EXT_20200330',
  'CLR_EXT_20200427',
  'CLR_EXT_20200525',
  'CLR_EXT_20200629',
  'CLR_EXT_20200729',
  'CLR_EXT_20200825',
  'CLR_EXT_20201130',
  'CLR_EXT_20201229',
  'CLR_EXT_20210128',
  'CLR_EXT_20210225',
  'CLR_EXT_20210430',
  'CLR_EXT_20210609',
  'CLR_EXT_20210628',
  'CLR_EXT_20210729',
  'CLR_EXT_20210830',
  'CLR_EXT_20210927',
  'CLR_EXT_20211028',
  'CLR_EXT_20211129',
  'CLR_EXT_20211230',
  'CLR_EXT_20220128',
  'CLR_EXT_20220228',
  'CLR_EXT_20220330',
  'CLR_EXT_20220430',
  'CLR_EXT_20220530',
  'CLR_EXT_20220627',
  'CLR_EXT_20220728',
  'CLR_EXT_20220829',
  'CLR_EXT_20220927',
  'CLR_EXT_20221028',
  'CLR_EXT_20221129',
  'CLR_EXT_20230103',
  'CLR_EXT_20230227',
  'BC_STAT_POPULATION_ESTIMATES_20230327',
  'CLR_EXT_20230426',
  'CLR_EXT_20230525',
  'CLR_EXT_20230626',
  'CLR_EXT_20230726',
  'CLR_EXT_20230828',
  'CLR_EXT_20230927',
  'CLR_EXT_20231030',
  'CLR_EXT_20231127',
  'CLR_EXT_20231227',
  'BC_STAT_POPULATION_ESTIMATES_20240129',
  'BC_STAT_POPULATION_ESTIMATES_20240226',
  'BC_STAT_POPULATION_ESTIMATES_20240326',
  'BC_STAT_POPULATION_ESTIMATES_20240429',
  'BC_STAT_POPULATION_ESTIMATES_20240527',
  'BC_STAT_POPULATION_ESTIMATES_20240628',
  'BC_STAT_POPULATION_ESTIMATES_20240726',
  'BC_STAT_POPULATION_ESTIMATES_20240827',
  'BC_STAT_POPULATION_ESTIMATES_20240926'
)

# Function to extract date parts from table name
extract_date_parts <- function(table_name) {
  date_part <- gsub(".*_([0-9]{8}).*", "\\1", table_name)
  year <- substr(date_part, 1, 4)
  month <- substr(date_part, 5, 6)
  day <- substr(date_part, 7, 8)
  list(year = year, month = month, day = day)
}

# Helper function to get the last day of a month
get_last_day <- function(year, month) {
  # Convert to numeric to handle edge cases
  month <- as.numeric(month)
  year <- as.numeric(year)

  # Handle December (month 12)
  if (month == 12) {
    next_month <- as.Date(paste0(year + 1, "-01-01")) # January of the next year
  } else {
    next_month <- as.Date(paste0(year, "-", sprintf("%02d", month + 1), "-01"))
  }

  last_day <- next_month - 1
  format(last_day, "%d")
}




############################################################################################
# Create missing EFF_DATE, END_DATA, and other columns in health client roster files
###################################################################################################
# Define the columns used in the indexes
columns_for_index <- c("STUDY_ID", "EFF_DATE", "POSTAL_CODE", "STREET_LINE")

# Function to generate index names based on table and columns
generate_index_name <- function(table, columns) {
  # Replace any special characters or spaces if necessary
  # Here, we assume table names and column names are clean
  index_name <- paste0("IDX_", table, "_", paste(columns, collapse = "_"))
  return(index_name)
}



# Iterate over table names and generate SQL for each
for (i in seq_along(table_names)) {
  table <- table_names[i]
  log_info(glue::glue("Start working on table: {table}"))

  # Drop the index if necessary

  # Generate ALTER INDEX statements
  # Assuming each table has one index following the naming convention
  # index_name <- generate_index_name(table, columns_for_index)
  # drop_index_sql <- paste0(
  #   "DROP INDEX IDX_", table, "_STUDY_ID_EFF_DATE_POSTAL_CODE_STREET_LINE ON dev.", table, ";"
  # )
  # dbExecute(decimal_conn, drop_index_sql)

  date_parts <- extract_date_parts(table)

  # According to ECON team, the actual year month is the month before the month in the file name.

  date_year_part <- as.numeric(date_parts$year)
  date_month_part <- as.numeric(date_parts$month)-1

  if (date_parts$month == "01"){
    date_year_part <- as.numeric(date_parts$year) -1
    date_month_part <- 12
  }

  # Determine if the date is before 202308
  table_date <- as.numeric(paste0(date_year_part, str_pad(date_month_part,width = 2, side = "left", pad = "0")))

  # Format EFF_DATE and END_DATE defaults
  eff_date_default <- paste0(date_year_part, "-", str_pad(date_month_part,width = 2, side = "left", pad = "0"), "-01")
  end_date_default <- paste0(date_year_part, "-", str_pad(date_month_part,width = 2, side = "left", pad = "0"), "-", get_last_day(date_year_part, date_month_part))

  log_info(glue::glue("Start working on table: {table}, the date is {table_date}"))
  # Generate SQL for updating columns and replacing NULL values
  if (table_date < 202307) {
    # For tables before 202307(table name is 20230827), set default EFF_DATE and END_DATE

    # # Generate SQL for adding new columns, only run once
    # alter_table_sql <- paste0(
    #   "ALTER TABLE dev.", table, " \n",
    #   "ADD \n",
    #   "    effective_year INT, \n",
    #   "    effective_month INT, \n",
    #   "    effective_day INT, \n",
    #   "    CHSA NVARCHAR(255) NULL, \n",
    #   "    LATITUDE FLOAT NULL, \n",
    #   "    LONGITUDE FLOAT NULL, \n",
    #   "    EFF_DATE DATE NULL, \n",
    #   "    END_DATE DATE NULL;\n"
    # )
    # # Execute the ALTER TABLE statement
    # dbExecute(decimal_conn, alter_table_sql)

    update_table_sql <- paste0(
      "UPDATE dev.", table, " \n",
      "SET \n",
      # "    effective_year = ", date_parts$year, ", \n",
      # "    effective_month = ", date_parts$month, ", \n",
      # "    effective_day = ", date_parts$day, ", \n",
      "    EFF_DATE = '", eff_date_default, "', \n",
      "    END_DATE = '", end_date_default, "';"
    )
    # Execute the UPDATE statement
    dbExecute(decimal_conn, update_table_sql)





  } else {
    # For tables after 202307, use ISNULL to handle missing values

    log_info(glue::glue("Start adding three columns: effective year, month, and day"))
    # # Generate SQL for adding new columns
    # alter_table_sql <- paste0(
    #   "ALTER TABLE dev.", table, " \n",
    #   "ADD \n",
    #   "    effective_year INT, \n",
    #   "    effective_month INT, \n",
    #   "    effective_day INT;\n"
    # )
    # #
    # # # Execute the ALTER TABLE statement
    # dbExecute(decimal_conn, alter_table_sql)

#  should not touch the raw data. only create new columns
    log_info(glue::glue("Start working on table: {table}"))
    update_table_sql <- paste0(
      "UPDATE dev.", table, " \n",
      "SET \n",
      # "    effective_year = ", date_parts$year, ", \n",
      # "    effective_month = ", date_parts$month, ", \n",
      # "    effective_day = ", date_parts$day, ", \n",
      "    EFF_DATE = CASE \n",
      "        WHEN EFF_DATE IS NULL THEN CAST('", eff_date_default, "' AS DATE) \n",
      "        ELSE EFF_DATE  \n",
      "    END, \n",
      "    END_DATE = CASE \n",
      "        WHEN END_DATE IS NULL THEN CAST('", end_date_default, "' AS DATE) \n",
      "        ELSE END_DATE  \n",
      "    END;"
    )
    # Execute the UPDATE statement
    dbExecute(decimal_conn, update_table_sql)




  }

  # Generate SQL for adding new columns for estimated date for each table, only run once after we add new data
  alter_table_sql <- paste0(
    "ALTER TABLE dev.", table, " \n",
    "ADD \n",
    "    ESTIMATED_EFF_DATE DATE NULL, \n",
    "    ESTIMATED_END_DATE DATE NULL;\n"
  )
  # Execute the ALTER TABLE statement
  dbExecute(decimal_conn, alter_table_sql)

  update_table_sql <- paste0(
    "UPDATE dev.", table, " \n",
    "SET \n",
    "    ESTIMATED_EFF_DATE = '", eff_date_default, "', \n",
    "    ESTIMATED_END_DATE = '", end_date_default, "';"
  )
  # Execute the UPDATE statement
  dbExecute(decimal_conn, update_table_sql)

  print(glue::glue("Finished table: {table}"))

}

# Disconnect from the database
# dbDisconnect(con)

#############################################################################################
# It is very slow to retrieve one study_id's records from the all those monthly health client roster tables. A view does not help.
# It is also slow to materialize the view.
# Now we first create indexes in those monthly health client roster tables
############################################################################################
# Define the columns to index
# Adjust this list if certain tables have different relevant columns
columns_to_index <- c("STUDY_ID")

# Function to generate CREATE INDEX statement
generate_create_index <- function(table, columns) {
  # Create a unique index name based on table and columns
  index_name <- paste0("IDX_", table, "_", paste(columns, collapse = "_"))

  # Construct the CREATE INDEX statement with proper quoting
  # Using square brackets to handle special characters or spaces in table/column names
  columns_str <- paste(paste0("[", columns, "]"), collapse = ", ")
  sql <- paste0(
    "CREATE INDEX [", index_name, "]\n",
    "ON [dev].[", table, "] (", columns_str, ");\n"
  )

  return(sql)
}

generate_drop_index <- function(table, columns) {
  # Create a unique index name based on table and columns
  index_name <- paste0("IDX_", table, "_", paste(columns, collapse = "_"))

  # Construct the CREATE INDEX statement with proper quoting
  # Using square brackets to handle special characters or spaces in table/column names
  sql <- paste0(
    "DROP INDEX IF EXISTS [", index_name, "]\n",
    "ON [dev].[", table, "] ;\n"
  )

  return(sql)
}

# Generate all CREATE INDEX statements
create_index_statements <- sapply(table_names, generate_create_index, columns = columns_to_index)
drop_index_statements <- sapply(table_names, generate_drop_index, columns = columns_to_index)


# Optionally, write the statements to a .sql file
writeLines(create_index_statements, con = "./mssql_etl/Indexes/Create_Indexes_all_monthly_health_client_tables.sql")

# Print the generated SQL statements
cat(create_index_statements, sep = "\n")


length(create_index_statements)
# Execute each CREATE INDEX statement
for (sql in create_index_statements) {
  print(sql)
  tryCatch({
    dbExecute(decimal_conn, sql)
    cat("Successfully created index.\n")
  }, error = function(e) {
    cat("Error creating index:", e$message, "\n")
  })
}



cat(drop_index_statements, sep = "\n")
length(drop_index_statements)
# Execute each CREATE INDEX statement
for (sql in drop_index_statements) {
  print(sql)
  tryCatch({
    dbExecute(decimal_conn, sql)
    cat("Successfully dropped index.\n")
  }, error = function(e) {
    cat("Error dropped index:", e$message, "\n")
  })
}

#####################################################################################
# update indexes
######################################################################################



# Initialize empty vectors to store SQL statements
update_stats_sql <- c()
rebuild_indexes_sql <- c()

# Iterate over each table to generate SQL statements
for (table in table_names) {

  # Generate UPDATE STATISTICS statement
  update_stat <- paste0(
    "UPDATE STATISTICS [dev].[", table, "];\n",
    "GO\n",
    "PRINT 'Statistics updated for [dev].[", table, "].';\n"
  )

  update_stats_sql <- c(update_stats_sql, update_stat)

  # Generate ALTER INDEX statements
  # Assuming each table has one index following the naming convention
  index_name <- generate_index_name(table, columns_for_index)

  rebuild_index <- paste0(
    "ALTER INDEX [", index_name, "]\n",
    "ON [dev].[", table, "]\n",
    "REBUILD;\n",
    "GO\n",
    "PRINT 'Index [", index_name, "] on [dev].[", table, "] rebuilt successfully.';\n"
  )

  rebuild_indexes_sql <- c(rebuild_indexes_sql, rebuild_index)
}

# Combine all statements
maintenance_sql <- c(update_stats_sql, rebuild_indexes_sql)

# Optional: Write the statements to a .sql file
writeLines(maintenance_sql, con = "Database_Maintenance_Tasks.sql")

################################################################################################
# (Optional) Execute the Maintenance Queries Directly from R
# If you prefer to execute the generated maintenance queries directly from R, you can extend the script to establish a database connection and run the queries. Caution: Executing these statements will modify your database. Ensure you have appropriate backups and permissions before proceeding.
################################################################################################




# Print a message indicating completion
cat("Maintenance SQL statements have been generated and saved to 'Database_Maintenance_Tasks.sql'.\n")

# Iterate over each table to execute maintenance tasks
for (table in table_names) {

  # Update Statistics
  update_stat <- paste0(
    "UPDATE STATISTICS [dev].[", table, "];"
  )

  update_print <- paste0(
    "PRINT 'Statistics updated for [dev].[", table, "].';"
  )

  # Rebuild Index
  index_name <- generate_index_name(table, columns_for_index)

  rebuild_index <- paste0(
    "ALTER INDEX [", index_name, "]\n",
    "ON [dev].[", table, "]\n",
    "REBUILD;"
  )

  rebuild_print <- paste0(
    "PRINT 'Index [", index_name, "] on [dev].[", table, "] rebuilt successfully.';"
  )

  # Combine statements
  maintenance_commands <- paste(update_stat, rebuild_index, sep = "\n")
  print_commands <- paste(update_print, rebuild_print, sep = "\n")

  # Execute UPDATE STATISTICS
  tryCatch({
    dbExecute(decimal_conn, maintenance_commands)
    dbExecute(decimal_conn, print_commands)
    cat("Successfully executed maintenance tasks for table:", table, "\n")
  }, error = function(e) {
    cat("Error executing maintenance tasks for table:", table, "\n", e$message, "\n")
  })

  # Optional: Pause between executions to manage load
  Sys.sleep(1) # Pause for 1 second
}

# Disconnect from the database
# dbDisconnect(con)

cat("All maintenance tasks have been executed.\n")

#############################################################################
# This query creates a big view with all the health client roster tables
# It turns out that we need index them, so we need to add the missing columns like EFF_DATE and END_DATE in those tables.
# then next query is created for creating index in those tables.
# after we create new column in previous query, now we can simplify this query for creating a unioned view.
####################################################################################
# Generate SQL query
sql_query <- "CREATE VIEW dev.VIEW_COMBINED_HEALTH_CLIENT AS"

for (i in seq_along(table_names)) {
  table <- table_names[i]

  sql_query <- paste0(
    sql_query,
    "  SELECT [STUDY_ID]
      ,[BIRTH_YR_MON]
      ,[SEX]
      ,[POSTAL_CODE]
      ,[CITY]
      ,[STREET_LINE]
      ,[LHA]
      ,[effective_year]
      ,[effective_month]
      ,[effective_day]
      ,[CHSA]
      ,[LATITUDE]
      ,[LONGITUDE]
      ,[EFF_DATE]
      ,[END_DATE]
      ,[ESTIMATED_EFF_DATE]
      ,[ESTIMATED_END_DATE]",
    "    FROM dev.", table
  )

  if (i < length(table_names)) {
    sql_query <- paste0(sql_query, "\n UNION ALL\n")
  } else {
    sql_query <- paste0(sql_query, "\n;")
  }
}



# Print the SQL query
cat(sql_query)

sql_query %>% write_lines("./mssql_etl/Views/view_union_all_health_client_file.sql")

dbExecute(decimal_conn, sql_query)



#####################################################################################
# use SQL Server's sys.dm_db_partition_stats and sys.allocation_units to calculate the size of each table in megabytes (MB).
#####################################################################################


# Function to sanitize table names for SQL queries
sanitize_table_names <- function(tables) {
  # Replace '.' with '].[' and wrap each table name with '[' and ']'
  # sanitized <- str_replace_all(tables, "\\.", "].[")
  sanitized <- paste0("[dev].[", tables, "]")
  return(sanitized)
}

# Sanitize table names to handle any special characters
sanitized_tables <- sanitize_table_names(table_names)

# Create a comma-separated list of sanitized table names for the SQL IN clause
tables_in_clause <- paste0("'", table_names, "'", collapse = ",")

# Define the SQL query to get table sizes
# This query retrieves the total reserved space, data space, index space, and unused space for each table
size_query <- sprintf("
  SELECT
      t.NAME AS TableName,
      SUM(p.rows) AS RowCounts,
      SUM(a.total_pages) * 8 AS TotalSpaceKB,
      SUM(a.used_pages) * 8 AS UsedSpaceKB,
      (SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS UnusedSpaceKB
  FROM
      sys.tables t
  INNER JOIN
      sys.indexes i ON t.OBJECT_ID = i.object_id
  INNER JOIN
      sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
  INNER JOIN
      sys.allocation_units a ON p.partition_id = a.container_id
  WHERE
      t.name IN (%s)
      AND i.index_id <= 1 -- Clustered index or heap
  GROUP BY
      t.NAME
  ORDER BY
      TotalSpaceKB DESC
", tables_in_clause)



# Execute the size query
size_data <- dbGetQuery(decimal_conn, size_query)

# Close the database connection as we no longer need it
# dbDisconnect(con)



# Convert KB to MB for easier readability
size_data <- size_data %>%
  mutate(
    TotalSpaceMB = TotalSpaceKB / 1024,
    UsedSpaceMB = UsedSpaceKB / 1024,
    UnusedSpaceMB = UnusedSpaceKB / 1024
  ) %>%
  select(TableName, RowCounts, TotalSpaceMB, UsedSpaceMB, UnusedSpaceMB)

# Display the size of each table
print(size_data)

# Calculate the total estimated storage required
total_storage_MB <- sum(size_data$TotalSpaceMB, na.rm = TRUE)
total_storage_GB <- total_storage_MB / 1024

cat(sprintf("Total Estimated Storage Required for Consolidation:\n%.2f MB (%.2f GB)\n",
            total_storage_MB, total_storage_GB))


# Assume PAGE compression with 4:1 ratio
compression_ratio <- 4

# Adjust total storage
adjusted_total_MB <- total_storage_MB / compression_ratio

cat(sprintf("Adjusted Estimated Storage with 4:1 Compression:\n%.2f MB (%.2f GB)\n",
            adjusted_total_MB, adjusted_total_MB / 1024))


########################################################################################
# Page Compression provides higher storage savings but at the cost of increased CPU usage.
# Read-Heavy Workloads: PAGE compression can improve I/O performance.
# Repetitive Data Patterns: PAGE compression is more effective for data with repetitive patterns.
# Write-Heavy Workloads: PAGE compression can increase CPU usage and slow down write operations.
########################################################################################


# Define your list of table names

# Define the compression type
compression_type <- "PAGE" # Change to "ROW" as needed

# Function to generate ALTER TABLE statements
generate_compression_sql <- function(schema_table, compression) {
  # parts <- str_split(schema_table, "\\.", simplify = TRUE)
  # schema <- parts[1]
  # table  <- parts[2]

  sql <- sprintf(
    "ALTER TABLE [%s].[%s] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = %s);",
    "dev",
    schema_table,
    compression
  )

  return(sql)
}

# Generate all ALTER TABLE statements
compression_sql <- sapply(table_names, generate_compression_sql, compression = compression_type)

# Optional: Review the generated SQL statements
print(compression_sql)

# Execute the compression statements
# Wrap in a try-catch to handle any errors without stopping the entire process
for (sql in compression_sql) {
  tryCatch({
    dbExecute(decimal_conn, sql)
    cat(sprintf("Successfully compressed table with SQL: %s\n", sql))
  }, error = function(e) {
    cat(sprintf("Error compressing table with SQL: %s\nError Message: %s\n", sql, e$message))
  })
}

# Disconnect from the database
# dbDisconnect(con)
cat("Database connection closed.\n")


###########################################################################################
# calculate one studyid address history a time and append to the aggregated table
##############################################################################################


# Load required libraries
library(DBI)
library(odbc)
library(dplyr)
library(lubridate)

# -----------------------------
# Database Connection Parameters
# -----------------------------

# Establish the database connection
con <- decimal_conn

# Verify connection
if (length(dbListTables(con)) == 0) {
  stop("No tables found in the database. Please check your connection parameters.")
} else {
  cat("Database connection established successfully.\n")
}

# -----------------------------
# Helper Function: Update Status
# -----------------------------
update_status <- function(con, study_id, new_status) {
  # Update the status column in the HEALTH_CLIENT_ID dimension table for the given study_id
  update_query <- sprintf("UPDATE dev.DIM_HEALTH_CLIENT_ID
                            SET Status = '%s'
                            WHERE STUDY_ID IN ('%s');", new_status, study_id)
  dbExecute(con, update_query)
  cat(sprintf("Updated STUDY_ID %s status to '%s'.\n", study_id, new_status))
}

# -----------------------------
# Helper Function: Process Each STUDY_ID
# -----------------------------
process_study_id <- function(con, study_id, log_df) {
  # Mark the study_id as 'Processing' before starting
  update_status(con, study_id, "Processing")
  start_time <- Sys.time()

  # Build the SQL script with the parameterized STUDY_ID
  sql_script <- sprintf("

    -- =============================================
    -- Step 1: Clean and Prepare Data (Using Staging Table)
    -- =============================================
    -- Drop temporary tables if they already exist
    IF OBJECT_ID('tempdb..#CleanedData') IS NOT NULL DROP TABLE #CleanedData;

    -- Select and transform data from the staging table
    SELECT
        S.STUDY_ID,
        POSTAL_CODE,
        STREET_LINE,
        ESTIMATED_EFF_DATE as EFF_DATE,
        ESTIMATED_END_DATE AS END_DATE,
        CASE
        WHEN STREET_LINE IS NOT NULL THEN
            CASE
                WHEN CHARINDEX(' ', STREET_LINE) > 0 THEN
                    LEFT(
                        STREET_LINE,
                        CHARINDEX(' ', STREET_LINE + ' ', CHARINDEX(' ', STREET_LINE + ' ') + 1) - 1
                    )
                ELSE STREET_LINE
            END
        ELSE STREET_LINE
        END AS STREET_FIRST_TWO_WORDS,
        ROW_NUMBER() OVER (PARTITION BY STUDY_ID ORDER BY ESTIMATED_EFF_DATE, ESTIMATED_END_DATE) AS RecordID
        INTO #CleanedData
    FROM [HealthFiles_test].[dev].[VIEW_COMBINED_HEALTH_CLIENT] S
    WHERE s.STUDY_ID IN ('%s')
    -- Create indexes to optimize subsequent operations
    CREATE NONCLUSTERED INDEX IX_CleanedData_EFF_DATE ON #CleanedData (STUDY_ID, RecordID);

    -- =============================================
    -- Step 2: Compute Previous Values Using Window Functions
    -- =============================================
    IF OBJECT_ID('tempdb..#LaggedData') IS NOT NULL DROP TABLE #LaggedData;
    SELECT
    CD.STUDY_ID,
    CD.POSTAL_CODE,
    CD.STREET_LINE,
    CD.STREET_FIRST_TWO_WORDS,
    CD.EFF_DATE,
    CD.END_DATE,
    CD.RecordID,
    LAG(CD.POSTAL_CODE) OVER (PARTITION BY CD.STUDY_ID ORDER BY  CD.RecordID) AS Prev_POSTAL_CODE,
    LAG(CD.STREET_LINE) OVER (PARTITION BY CD.STUDY_ID ORDER BY  CD.RecordID) AS Prev_STREET_LINE,
    LAG(CD.STREET_FIRST_TWO_WORDS ) OVER (PARTITION BY CD.STUDY_ID ORDER BY  CD.RecordID) AS Prev_STREET_FIRST_TWO_WORDS
    INTO #LaggedData
    FROM  #CleanedData CD
    ORDER BY CD.STUDY_ID, CD.RecordID;
    CREATE NONCLUSTERED INDEX IX_LaggedData_STUDY_ID_RecordID ON #LaggedData (STUDY_ID, RecordID);

    -- =============================================
    -- Step 3: Flag Changes in Address
    -- =============================================
    IF OBJECT_ID('tempdb..#ChangeFlagData') IS NOT NULL DROP TABLE #ChangeFlagData;
    SELECT
        LD.STUDY_ID,
        LD.POSTAL_CODE,
        LD.STREET_LINE,
        LD.STREET_FIRST_TWO_WORDS,
        LD.EFF_DATE,
        LD.END_DATE,
        LD.RecordID,
        CASE
            WHEN LD.Prev_POSTAL_CODE IS NULL THEN 1
            WHEN ISNULL(LD.Prev_POSTAL_CODE, '') != ISNULL(LD.POSTAL_CODE, '')   THEN 1  -- it is rare that postal code is null
            -- Compare street_line values treating NULL as an empty string.
            -- WHEN ISNULL(LD.Prev_STREET_LINE, '') <> ISNULL(LD.STREET_LINE, '') THEN 1  -- STREET LINE are messy, could have typo etc.
            WHEN ISNULL(LD.Prev_STREET_FIRST_TWO_WORDS, '') <> ISNULL(LD.STREET_FIRST_TWO_WORDS, '') THEN 1  -- STREET_FIRST_TWO_WORDS are much clean
            ELSE 0
        END AS ChangeFlag
     INTO #ChangeFlagData
    FROM #LaggedData LD
    ORDER BY LD.STUDY_ID,LD.RecordID;
    CREATE NONCLUSTERED INDEX IX_ChangeFlagData_STUDY_ID_RecordID ON #ChangeFlagData (STUDY_ID, RecordID);


    -- =============================================
    -- Step 4: Assign GroupAddressKey Using Cumulative Sum
    -- =============================================
    IF OBJECT_ID('tempdb..#GroupedKeyData') IS NOT NULL DROP TABLE #GroupedKeyData;
    SELECT
        CFD.STUDY_ID,
        CFD.POSTAL_CODE,
        CFD.STREET_LINE,
        CFD.STREET_FIRST_TWO_WORDS,
        CFD.EFF_DATE,
        CFD.END_DATE,
        CFD.RecordID,
        CFD.ChangeFlag,
        -- Cumulative sum to assign GroupAddressKey
        SUM(CASE WHEN CFD.ChangeFlag = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY CFD.STUDY_ID ORDER BY CFD.RecordID ROWS UNBOUNDED PRECEDING) AS GroupAddressKey
    INTO #GroupedKeyData
    FROM #ChangeFlagData CFD
    ORDER BY CFD.STUDY_ID, CFD.RecordID;
    CREATE NONCLUSTERED INDEX IX_GroupedKeyData_GroupAddressKey ON #GroupedKeyData (GroupAddressKey);

    -- =============================================
    -- Step 5: Aggregate Grouped Data
    -- =============================================
    IF OBJECT_ID('tempdb..#GroupedData') IS NOT NULL DROP TABLE #GroupedData;
    SELECT
        GK.STUDY_ID,
        GK.POSTAL_CODE,
        GK.STREET_FIRST_TWO_WORDS,
        MAX(GK.STREET_LINE) AS STREET_LINE,
        MIN(GK.EFF_DATE) AS EFF_DATE,
        MAX(GK.END_DATE) AS END_DATE,
        GK.GroupAddressKey
    INTO #GroupedData
    FROM #GroupedKeyData GK
    GROUP BY GK.STUDY_ID, GK.GroupAddressKey, GK.POSTAL_CODE, GK.STREET_FIRST_TWO_WORDS;

    -- =============================================
    -- Step 6: Insert Aggregated Data into Target Table (Optimized Join)
    -- =============================================
    DELETE D
    FROM DEV.FCT_HEALTH_CLIENT_ADDRESS_HISTORY D
    INNER JOIN #GroupedData GD
        ON D.STUDY_ID = GD.STUDY_ID;

    INSERT INTO DEV.FCT_HEALTH_CLIENT_ADDRESS_HISTORY (
        STUDY_ID,
        GroupAddressKey,
        POSTAL_CODE,
        STREET_LINE,
        EFF_DATE,
        END_DATE,
        CITY,
        LATITUDE,
        LONGITUDE
    )
    SELECT
        GD.STUDY_ID,
        GD.GroupAddressKey,
        GD.POSTAL_CODE,
        GD.STREET_LINE,
        GD.EFF_DATE,
        GD.END_DATE,
        B.[CITY],
        B.LATITUDE,
        B.LONGITUDE
    FROM #GroupedData GD
    LEFT JOIN dev.DIM_HEALTH_CLIENT_ADDRESS B
        ON GD.POSTAL_CODE = B.POSTAL_CODE
        AND GD.STREET_LINE = B.STREET_LINE
        ORDER BY GD.STUDY_ID,
        GD.GroupAddressKey;

    -- =============================================
    -- Step 7: Cleanup Temporary Tables
    -- =============================================
    DROP TABLE IF EXISTS #CleanedData, #LaggedData, #ChangeFlagData, #GroupedKeyData, #GroupedData;
  ", study_id)

  # Execute the SQL script with error handling
  tryCatch({
    dbExecute(con, sql_script)
    # If successful, mark the study_id as 'Processed'
    update_status(con, study_id, "Processed")
    end_time <- Sys.time()
    duration <- as.numeric(difftime(end_time, start_time, units = "secs"))
    log_df <- log_df %>% add_row(
      STUDY_ID      = study_id,
      Status        = "Success",
      Start_Time    = start_time,
      End_Time      = end_time,
      Duration_secs = duration,
      Message       = "Processed successfully."
    )
    cat(sprintf("STUDY_ID %s processed successfully in %.2f seconds.\n", study_id, duration))
  }, error = function(e) {
    # On error, update the status accordingly and log the error
    update_status(con, study_id, "Error")
    end_time <- Sys.time()
    duration <- as.numeric(difftime(end_time, start_time, units = "secs"))
    log_df <<- log_df %>% add_row(
      STUDY_ID      = study_id,
      Status        = "Failed",
      Start_Time    = start_time,
      End_Time      = end_time,
      Duration_secs = duration,
      Message       = e$message
    )
    cat(sprintf("Error processing STUDY_ID %s: %s\n", study_id, e$message))
  })

  return(log_df)
}

# -----------------------------
# Main Processing Loop
# -----------------------------
# Fetch the total count of study_ids in the dev.DIM_HEALTH_CLIENT_ID table
total_ids <- dbGetQuery(con, "SELECT COUNT(*) AS TotalIDs FROM dev.DIM_HEALTH_CLIENT_ID WHERE Status = 'Pending'")$TotalIDs
cat(sprintf("Total STUDY_IDs to process: %d\n", total_ids))
#


# Initialize an empty dataframe for logging
log_df <- tibble(
  STUDY_ID      = character(),
  Status        = character(),
  Start_Time    = as.POSIXct(character()),
  End_Time      = as.POSIXct(character()),
  Duration_secs = numeric(),
  Message       = character()
)



iteration <- 0

repeat {
  # Fetch one pending STUDY_ID record from the database
  pending_record <- dbGetQuery(con, "SELECT TOP 1 STUDY_ID, Status FROM dev.DIM_HEALTH_CLIENT_ID WHERE Status = 'Pending'")

  # If no pending record is found, exit the loop
  if (nrow(pending_record) == 0) {
    cat("No pending STUDY_ID found. Exiting processing loop.\n")
    break
  }

  study_id <- pending_record$STUDY_ID[1]

  # Optional: check status (should be 'Pending')
  if (tolower(pending_record$Status[1]) != "pending") {
    cat(sprintf("STUDY_ID %s is not pending. Skipping...\n", study_id))
    next
  }

  iteration <- iteration + 1
  cat(sprintf("\nIteration %d: Processing STUDY_ID = %s\n", iteration, study_id))

  # Process the current STUDY_ID
  log_df <- process_study_id(con, study_id, log_df)

  # Save the log after every 10,000 iterations
  # if (iteration %% 10000 == 0) {
  #   write.csv(log_df, "processing_log.csv", row.names = FALSE)
  #   cat(sprintf("Log saved after %d iterations.\n", iteration))
  # }
}


# Process each STUDY_ID sequentially
# for (i in 1:total_ids) {
#   current_id <- study_ids[i]
#   cat(sprintf("\nProcessing %d of %d: STUDY_ID = %s\n", i, total_ids, current_id))
#
#   # Process the current STUDY_ID and update log_df
#   log_df <- process_study_id(con, current_id, log_df)
#
#   # Save the log after each iteration
#   # Save the log after every 10,000 iterations
#   if (i %% 10000 == 0) {
#     write.csv(log_df, "\\\\Client\\G$\\Operations\\Data Science and Analytics\\2024_bcstats_db\\mssql_etl\\processing_log.csv", row.names = FALSE)
#     cat(sprintf("Log saved after %d iterations.\n", i))
#   }
# }

# Display and save the final log
# print(log_df)


write.csv(log_df, "\\\\Client\\G$\\Operations\\Data Science and Analytics\\2024_bcstats_db\\mssql_etl\\processing_log_final.csv", row.names = FALSE)

# Disconnect from the database
dbDisconnect(con)
cat("Database connection closed.\n")



###########################################################################################
# consolidate  88 monthly tables into a single compressed table in SQL Server without transferring data through R, you can leverage R to execute T-SQL queries directly on the server. This approach ensures that data movement remains within the database environment, optimizing performance and minimizing network overhead.
##############################################################################################

# monthly_tables <-  table_names
#
#
#
# # Verify that all tables exist in the database
# existing_tables_query <- sprintf("
#   SELECT TABLE_NAME
#   FROM INFORMATION_SCHEMA.TABLES
#   WHERE TABLE_SCHEMA = 'dev'
#     AND TABLE_NAME IN (%s);
# ", paste(sprintf("'%s'", monthly_tables), collapse = ", "))
#
# existing_tables <- dbGetQuery(decimal_conn, existing_tables_query)$TABLE_NAME
#
# missing_tables <- setdiff(monthly_tables, existing_tables)
#
# if (length(missing_tables) > 0) {
#   warning(sprintf("The following tables are missing in the 'dev' schema and will be skipped: %s",
#                   paste(missing_tables, collapse = ", ")))
#   # Remove missing tables from the list
#   monthly_tables <- setdiff(monthly_tables, missing_tables)
# }
#
# cat(sprintf("Proceeding with %d tables for consolidation.\n", length(monthly_tables)))
#
# # Create Consolidated Table with Compression
#
# # Define the name of the consolidated table
# consolidated_table <- "TBL_COMBINED_HEALTH_CLIENT"
#
# # Check if the consolidated table already exists
# check_consolidated_query <- sprintf("
#   SELECT COUNT(*) AS Count
#   FROM INFORMATION_SCHEMA.TABLES
#   WHERE TABLE_SCHEMA = 'dev'
#     AND TABLE_NAME = '%s';
# ", consolidated_table)
#
# consolidated_exists <- dbGetQuery(decimal_conn, check_consolidated_query)$Count > 0
#
# if (consolidated_exists) {
#   stop(sprintf("The table '%s' already exists in the 'dev' schema. Please choose a different name or drop the existing table before proceeding.",
#                consolidated_table))
# } else {
#   # Define the schema as a string. Adjust data types as per your tables.
#   # Example schema based on your VIEW_COMBINED_HEALTH_CLIENT
#   consolidated_schema <- "
#     STUDY_ID VARCHAR(255) NOT NULL,
#     POSTAL_CODE VARCHAR(255),
#     STREET_LINE VARCHAR(255),
#     EFF_DATE DATE,
#     END_DATE DATE
#   "
#
#   # Create the consolidated table with compression
#   create_table_query <- sprintf("
#     CREATE TABLE dev.%s (
#       %s
#     )
#     WITH (DATA_COMPRESSION = PAGE); -- Choose ROW or PAGE based on your preference
#   ", consolidated_table, consolidated_schema)
#
#   # Execute the table creation
#   tryCatch({
#     dbExecute(decimal_conn, create_table_query)
#     cat(sprintf("Consolidated table '%s' created successfully with PAGE compression.\n", consolidated_table))
#   }, error = function(e) {
#     stop(sprintf("Failed to create consolidated table '%s'. Error: %s", consolidated_table, e$message))
#   })
# }
#
# # Define Batch Insertion Function
#
# # Define the batch insertion function
# insert_table_in_batches <- function(con, source_table, target_table, batch_size = 10000) {
#   # Initialize logging variables
#   start_time <- Sys.time()
#
#   # Define the T-SQL script for batch insertion
#   # Using TRY-CATCH for error handling and transactions for batch management
#   insert_script <- sprintf("
#     BEGIN TRY
#       BEGIN TRANSACTION;
#
#       -- Insert data from source_table to target_table in batches
#       DECLARE @BatchSize INT = %d;
#       DECLARE @Offset INT = 0;
#       DECLARE @TotalRows INT;
#
#       SELECT @TotalRows = COUNT(*) FROM dev.%s;
#
#       WHILE (@Offset < @TotalRows)
#       BEGIN
#         INSERT INTO dev.%s (
#           STUDY_ID,
#           POSTAL_CODE,
#           STREET_LINE,
#           EFF_DATE,
#           END_DATE
#         )
#         SELECT
#           STUDY_ID,
#           POSTAL_CODE,
#           STREET_LINE,
#           EFF_DATE,
#           END_DATE
#         FROM dev.%s
#         ORDER BY STUDY_ID
#         OFFSET @Offset ROWS FETCH NEXT @BatchSize ROWS ONLY;
#
#         SET @Offset = @Offset + @BatchSize;
#       END
#
#       COMMIT TRANSACTION;
#       SELECT 'Success' AS Status, @TotalRows AS InsertedRows, DATEDIFF(SECOND, '%s', GETDATE()) AS DurationSeconds;
#     END TRY
#     BEGIN CATCH
#       IF @@TRANCOUNT > 0
#         ROLLBACK TRANSACTION;
#
#       SELECT
#         ERROR_NUMBER() AS ErrorNumber,
#         ERROR_SEVERITY() AS ErrorSeverity,
#         ERROR_STATE() AS ErrorState,
#         ERROR_PROCEDURE() AS ErrorProcedure,
#         ERROR_LINE() AS ErrorLine,
#         ERROR_MESSAGE() AS ErrorMessage;
#     END CATCH
#   ", batch_size, source_table, target_table, source_table, start_time)
#
#   # Execute the insert script
#   result <- dbGetQuery(con, insert_script)
#
#   # Determine the status based on the result
#   if ("Status" %in% colnames(result)) {
#     return(list(status = result$Status, inserted = result$InsertedRows, duration = result$DurationSeconds))
#   } else {
#     # An error occurred; extract error details
#     error_message <- paste(result$ErrorNumber, result$ErrorSeverity, result$ErrorState, result$ErrorProcedure, result$ErrorLine, result$ErrorMessage, sep = " | ")
#     return(list(status = paste("Failed:", error_message), inserted = 0, duration = 0))
#   }
# }
#
# # Insert Data from Monthly Tables in Batches
#
# # Initialize a log dataframe to track the insertion process
# insertion_log <- data.frame(
#   Table = character(),
#   Status = character(),
#   Inserted_Rows = integer(),
#   Duration_secs = numeric(),
#   stringsAsFactors = FALSE
# )
#
# # Iterate over each monthly table and insert data in batches
# for (source_table in monthly_tables[1]) {
#   cat(sprintf("Starting insertion from '%s'...\n", source_table))
#
#   # Call the batch insertion function
#   result <- insert_table_in_batches(decimal_conn, source_table, consolidated_table, batch_size = 10000)
#
#   # Append the result to the log
#   insertion_log <- insertion_log %>%
#     add_row(
#       Table = source_table,
#       Status = result$status,
#       Inserted_Rows = result$inserted,
#       Duration_secs = result$duration
#     )
#
#   # Provide feedback
#   if (result$status == "Success") {
#     cat(sprintf("Successfully inserted %d rows from '%s' in %.2f seconds.\n\n",
#                 result$inserted, source_table, result$duration))
#   } else {
#     cat(sprintf("Failed to insert from '%s'. Error Details: %s\n\n",
#                 source_table, result$status))
#   }
# }
#
# # Review and Save Insertion Log
#
# # Display the insertion log
# print(insertion_log)
#
# # Optionally, save the log to a CSV file for record-keeping
# write.csv(insertion_log, "insertion_log.csv", row.names = FALSE)
# cat("Insertion log saved to 'insertion_log.csv'.\n")
#
# # Close the database connection
# dbDisconnect(decimal_conn)
# cat("Database connection closed.\n")



# only 14 tables (after 20230827) have the EFF_DATE

# -- Example SQL query to verify indexes on a specific table
# SELECT
#     ind.name AS IndexName,
#     ind.type_desc AS IndexType,
#     col.name AS ColumnName
# FROM
#     sys.indexes ind
# INNER JOIN
#     sys.index_columns ic ON ind.object_id = ic.object_id AND ind.index_id = ic.index_id
# INNER JOIN
#     sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id
# WHERE
#     ind.object_id = OBJECT_ID('dev.CLR_EXT_20200213_for_201107') -- Replace with your table name
# ORDER BY
#     ind.name, ic.key_ordinal;
