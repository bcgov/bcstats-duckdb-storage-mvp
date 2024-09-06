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

if(!require(pacman)){
  install.packages("pacman")
}

pacman::p_load(odbc, tidyverse, config, DBI, dbplyr,nanoarrow, arrow, duckdb)

# we save the path to a config file
test_csv_folder = config::get("test_sql_server_csv")
list.files(test_csv_folder, pattern = "*.csv", full.names = T)[1]
##################################################################################
# Test loading a big file to MS sql server
##################################################################################

con <-dbConnect(odbc::odbc(),
                Driver = config::get("Driver"),
                Server = config::get("Server"),
                Database = config::get("Database"),
                Trusted_Connection = "True"
)

# Build a vector of table names: tables
tables <- dbListTables(con)
# tables have the all table names
dbGetInfo(con)
# Get DBMS metadata

# dbSendQuery()
# Execute a query on a given database connection
# dbSendStatement()
# Execute a data manipulation statement on a given database connection

# In SQL server:
# To rename a column:
#
#   sp_rename 'table_name.old_column_name', 'new_column_name' , 'COLUMN';
# To rename a table:
#
#   sp_rename 'old_table_name','new_table_name';

# rename new data by remove "day"
DBI::dbExecute(con, "exec sp_rename 'IDIR\\JDUAN.BC_Stat_CLR_EXT_20230525', 'BC_Stat_CLR_EXT_202305' ;")

# dbWriteTableArrow()
# # Copy Arrow objects to database tables
# dbCreateTableArrow()
# # Create a table in the database based on an Arrow object


# read data
# data <- open_dataset(sources = flnm,
#                      format = format,
#                      schema = schema,
#                      skip = 1)


data <- read_csv_arrow(file = list.files(test_csv_folder, full.names = T)[1])
data %>% glimpse()

# write to sql server
tictoc::tic()
DBI::dbWriteTableArrow(con,
                       name = "BC_Stat_Population_Estimates_20240527",
                       nanoarrow::as_nanoarrow_array_stream(data)
                       # append = append
)

tictoc::toc()
# 590.52 sec elapsed




dbDisconnect(con)

# to repeat this process, we can put them in a function.

dbDisconnect(con)

load_csv_save_db = function(con,file_path, table_name){
  # ulitize the arrow to load the CSV files and write to a database, which in this case is decimal database.
  data <- read_csv_arrow(file = file_path)
  data %>% glimpse()

  # write to sql server
  tictoc::tic()
  DBI::dbWriteTableArrow(con,
                         name = table_name,
                         nanoarrow::as_nanoarrow_array_stream(data)
                         # append = append
  )

  tictoc::toc()

  # dbDisconnect(con)
}


# as long as we know the csv file path, we can load them in arrow and save arrow object to sql server.
load_csv_save_db(con,
                 file_path = list.files(test_csv_folder, pattern = "*.csv", full.names = T)[2],
                 table_name = "BC_Stat_Population_Estimates_2024082")


#################################################################################
#################################################################################
# second dataset
# data <- read_csv_arrow(file = list.files(test_csv_folder,pattern = "*.csv", full.names = T)[2])
# data %>% glimpse()
#
#
# # write to sql server using arrow helps
# tictoc::tic()
# DBI::dbWriteTableArrow(con,
#                        name = "BC_Stat_CLR_EXT_20230525",
#                        nanoarrow::as_nanoarrow_array_stream(data)
#                        # append = append
# )
#
# tictoc::toc()
# 412.19 sec elapsed

load_csv_save_db(con,
                 file_path = list.files(test_csv_folder, pattern = "*.csv", full.names = T)[3],
                 table_name = "BC_Stat_Population_Estimates_2024082")

##################################################################################
# Test loading a big file to duckdb
##################################################################################
# write to arrow dataset
# tictoc::tic()
# write_dataset(dataset =  data %>% group_by(LHA),
#               path = paste0(test_csv_folder, "/BC_Stat_Population_Estimates_20240527"),
#               format = "parquet"
# )
# tictoc::toc()
# # 6.2 sec elapsed
#
#
# ##################################################################################
# # Test loading a big file to duckdb
# ##################################################################################
# # write to arrow dataset
# tictoc::tic()
# write_dataset(dataset =  data %>% group_by(LHA),
#               path = paste0(test_csv_folder, "/BC_Stat_CLR_EXT_20230525"),
#               format = "parquet"
# )
# tictoc::toc()
# # 3.08 sec elapsed
#
#
#
# ##################################################################################
# # Using duckdb to join data
# ##################################################################################
#
#
#
# BC_Stat_Population_Estimates_20240527 = open_dataset(paste0(test_csv_folder, "/BC_Stat_Population_Estimates_20240527"))
#
# BC_Stat_CLR_EXT_20230525 = open_dataset(paste0(test_csv_folder, "/BC_Stat_CLR_EXT_20230525"))
#
# BC_Stat_Population_Estimates_20240527 %>%
#   glimpse()
#
# BC_Stat_CLR_EXT_20230525 %>%
#   glimpse()


##################################################################################
# Test loading a big CSV file to duckdb database directly
##################################################################################


# Create a connection to a new DuckDB database file
bcstats_con <- duckdb::dbConnect(duckdb::duckdb(), paste0(test_csv_folder, "/bcstats_db.duckdb"))
dbListTables(bcstats_con)

load_csv_save_db(bcstats_con,
                 file_path = list.files(test_csv_folder, pattern = "*.csv", full.names = T)[3],
                 table_name = "BC_Stat_Population_Estimates_2024082")


# Load a CSV file into DuckDB
tictoc::tic()
duckdb::duckdb_read_csv(
  conn = bcstats_con,
  name = "BC_Stat_Population_Estimates_202408",       # Name of the table to create
  files = list.files(test_csv_folder, pattern = "*.csv",full.names = T)[2],  # Path to the CSV file
  header = TRUE,           # Whether the CSV file has a header row
  delim = ",",             # Delimiter used in the CSV file
  quote = "\"",            # Quote character used in the CSV file
  na.strings = "",         # Strings to interpret as NA
  transaction = TRUE       # Whether to wrap the operation in a transaction
)
tictoc::toc()
# 10.61 sec elapsed
# Error: rapi_execute: Failed to run query
# Error: FATAL Error: Failed to create checkpoint because of error: INTERNAL Error: Unsupported compression function type

# Query the table
result <- dbGetQuery(bcstats_con, "SELECT * FROM BC_Stat_Population_Estimates_20240527 ")
print(result)



# Load a CSV file into DuckDB
# tictoc::tic()
# duckdb::duckdb_read_csv(
#   conn = bcstats_con,
#   name = "BC_Stat_CLR_EXT_20230525",       # Name of the table to create
#   files = list.files(test_csv_folder, pattern = "*.csv",full.names = T)[3],  # Path to the CSV file
#   header = TRUE,           # Whether the CSV file has a header row
#   delim = ",",             # Delimiter used in the CSV file
#   quote = "\"",            # Quote character used in the CSV file
#   na.strings = "",         # Strings to interpret as NA
#   transaction = TRUE       # Whether to wrap the operation in a transaction
# )
# tictoc::toc()
# 7.28 sec elapsed



# Query the table
result <- dbGetQuery(bcstats_con, "SELECT * FROM BC_Stat_CLR_EXT_20230525")
print(result)

## write a table to it
dbWriteTable(bcstats_con, "iris", iris)


# create / connect to database file
drv <- duckdb(dbdir = "C:\\Users\\JDUAN\\OneDrive - Government of BC\\2024-025 Brett and Jon Database Test Warehouse/backup/bcstats_db.duckdb")
bcstats_con <- dbConnect(drv)

# Show how many tables in database
dbListTables(bcstats_con)
# many tables.
# list columns/fields in one table
dbListFields(bcstats_con, "BC_Stat_Population_Estimates_20240527")

dbListFields(bcstats_con, "BC_Stat_CLR_EXT_20230525")



dbDisconnect(bcstats_con, shutdown = TRUE)


# New code should prefer dbCreateTable() and dbAppendTable().

mtcars

dbWriteTable(con, name = "test_mtcars_4", value = mtcars )

