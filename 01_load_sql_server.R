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


# Define the path to the test CSV folder
# This path is retrieved from the configuration file
test_csv_folder = config::get("test_sql_server_csv")

# List all CSV files in the test folder
# This will return a character vector of file paths
csv_files = list.files(test_csv_folder, pattern = "*.csv", full.names = TRUE)

# Print the number of CSV files found
cat("Number of CSV files found:", length(csv_files), "\n")

# Print the first file path (if any files were found)
if (length(csv_files) > 0) {
  cat("First CSV file:", csv_files[1], "\n")
} else {
  cat("No CSV files found in the specified folder.\n")
}


##################################################################################
# Test loading a big file to MS sql server
##################################################################################

# Connect to SQL Server
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

# following are the functions to execute a query on a given database connection
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
# DBI::dbExecute(con, "exec sp_rename 'IDIR\\JDUAN.BC_Stat_CLR_EXT_20230525', 'BC_Stat_CLR_EXT_202305' ;")

# dbWriteTableArrow()
# # Copy Arrow objects to database tables
# dbCreateTableArrow()
# # Create a table in the database based on an Arrow object

# following are the functions to read data from a arrow database table
# read data
# data <- open_dataset(sources = flnm,
#                      format = format,
#                      schema = schema,
#                      skip = 1)

# read data from a csv file using arrow facility
data <- read_csv_arrow(file = csv_files[1])

# show the data structure
data %>% glimpse()

# write to sql server

# use tictoc to time the process
tictoc::tic()
DBI::dbWriteTableArrow(con,
                       name = "BC_Stat_Population_Estimates_202408",
                       nanoarrow::as_nanoarrow_array_stream(data)
                       # append = append
)

tictoc::toc()
# 590.52 sec elapsed

# disconnect from the database
dbDisconnect(con)

# to repeat this process, we can put them in a function.

# define a function to load a csv file and save to a database
load_csv_save_db = function(con, file_path, table_name){
  # allow the function to accept a connection object, file path and table name

  # read the csv file into an arrow object,  
  # ulitize the arrow to load the CSV files and write to a database, which in this case is sql server database.
  data <- arrow::read_csv_arrow(file = file_path)
  # show the data structure
  data %>% glimpse()

  # write to sql server
  tictoc::tic()
  DBI::dbWriteTableArrow(con,
                         name = table_name,
                         nanoarrow::as_nanoarrow_array_stream(data)
                         # append = append
  )
  tictoc::toc()
  # disconnect from the database
  DBI::dbDisconnect(con)
}


# as long as we know the csv file path, we can load them in arrow and save arrow object to sql server.
load_csv_save_db(file_path = list.files(test_csv_folder, pattern = "*.csv", full.names = T)[2],
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
load_csv_save_db(file_path = list.files(test_csv_folder, pattern = "*.csv", full.names = T)[3],
                 table_name = "BC_Stat_Population_Estimates_2024082")

##################################################################################
# Test loading a big file to duckdb
# The following code is to test the performance of loading a big file to duckdb
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


