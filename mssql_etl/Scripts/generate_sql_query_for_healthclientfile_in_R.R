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
  'CLR_EXT_202201028',
  'CLR_EXT_202201129',
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
  'bc_stat_population_estimates_20240926'
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
  eff_date_default <- paste0(date_year_part, "-", date_month_part, "-01")
  end_date_default <- paste0(date_year_part, "-", date_month_part, "-", get_last_day(date_year_part, date_month_part))

  log_info(glue::glue("Start working on table: {table}, the date is {table_date}"))
  # Generate SQL for updating columns and replacing NULL values
  if (table_date < 202307) {
    # For tables before 202307(table name is 20230827), set default EFF_DATE and END_DATE

    # Generate SQL for adding new columns, only run once
    alter_table_sql <- paste0(
      "ALTER TABLE dev.", table, " \n",
      "ADD \n",
      "    effective_year INT, \n",
      "    effective_month INT, \n",
      "    effective_day INT, \n",
      "    CHSA NVARCHAR(255) NULL, \n",
      "    LATITUDE FLOAT NULL, \n",
      "    LONGITUDE FLOAT NULL, \n",
      "    EFF_DATE DATE NULL, \n",
      "    END_DATE DATE NULL;\n"
    )
    # Execute the ALTER TABLE statement
    dbExecute(decimal_conn, alter_table_sql)

    update_table_sql <- paste0(
      "UPDATE dev.", table, " \n",
      "SET \n",
      "    effective_year = ", date_parts$year, ", \n",
      "    effective_month = ", date_parts$month, ", \n",
      "    effective_day = ", date_parts$day, ", \n",
      "    EFF_DATE = '", eff_date_default, "', \n",
      "    END_DATE = '", end_date_default, "';"
    )
    # Execute the UPDATE statement
    dbExecute(decimal_conn, update_table_sql)


  } else {
    # For tables after 202307, use ISNULL to handle missing values

    log_info(glue::glue("Start adding three columns: effective year, month, and day"))
    # Generate SQL for adding new columns
    alter_table_sql <- paste0(
      "ALTER TABLE dev.", table, " \n",
      "ADD \n",
      "    effective_year INT, \n",
      "    effective_month INT, \n",
      "    effective_day INT;\n"
    )
    #
    # # Execute the ALTER TABLE statement
    dbExecute(decimal_conn, alter_table_sql)

#  should not touch the raw data. only create new columns
    log_info(glue::glue("Start working on table: {table}"))
    update_table_sql <- paste0(
      "UPDATE dev.", table, " \n",
      "SET \n",
      "    effective_year = ", date_parts$year, ", \n",
      "    effective_month = ", date_parts$month, ", \n",
      "    effective_day = ", date_parts$day, ", \n",
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
columns_to_index <- c("STUDY_ID", "EFF_DATE", "POSTAL_CODE", "STREET_LINE")

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

# Generate all CREATE INDEX statements
create_index_statements <- sapply(table_names, generate_create_index, columns = columns_to_index)

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
      ,[END_DATE] ",
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

# only 14 tables have the EFF_DATE

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
