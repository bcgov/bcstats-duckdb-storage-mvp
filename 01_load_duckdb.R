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

# import functions from utils.r
source(file = "./utils/utils.r")

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

# retrieve the file name from the file path, which is the table name
table_names = basename(csv_files) %>% str_remove(".csv")
# some table names are not valid in duckdb, we need to change them
table_names = table_names %>% clean_table_names() # clean_table_names is from utils.r
# # Done: clean the table names, roll back one month, rules,


##################################################################################
# Test loading a big CSV file to duckdb database directly
##################################################################################


# Create a connection to a new DuckDB database file
# The function will create a duckdb database file in the test_csv_folder if there is no such file
bcstats_con <- duckdb::dbConnect(duckdb::duckdb(),
                                file.path(test_csv_folder, "bcstats_db.duckdb"))

## test connection by write a small table to it
# dbWriteTable(bcstats_con, "iris", iris)

# list all tables in the database
dbListTables(bcstats_con)


file_path = csv_files[2]
table_name = table_names[2]
con = bcstats_con

##################################################################################
# First method using the custom function
##################################################################################
# load a csv file to the database using custom function
tictoc::tic()

data <- arrow::read_csv_arrow(file = file_path)
# show the data structure
# data %>% glimpse()

# write to sql server



load_csv_save_db(bcstats_con,
                 file_path = csv_files[2],
                 table_name = table_names[2])
tictoc::toc()

# 35.24 sec elapsed
##################################################################################
# If there is an error: Error: FATAL Error: Failed to create checkpoint because of error: INTERNAL Error: Unsupported compression function type
# 1. Update the arrow and duckdb packages: install.packages(c("arrow", "duckdb", "duckdbfs", "duckplyr"))
# 2. Restart R session: Ctrl+ Shift + F10
# 3. Close other applications to get more ram
##################################################################################



##################################################################################
# Second method using the custom function
##################################################################################
# second solution is using duckdb_read_csv function
tictoc::tic()
duckdb::duckdb_read_csv(
  conn = bcstats_con,
  name = table_names[2],       # Name of the table to create
  files = csv_files[2],  # Path to the CSV file
  header = TRUE,           # Whether the CSV file has a header row
  delim = ",",             # Delimiter used in the CSV file
  quote = "\"",            # Quote character used in the CSV file
  na.strings = "",         # Strings to interpret as NA
  transaction = TRUE       # Whether to wrap the operation in a transaction
)
tictoc::toc()
# 10.61 sec elapsed
# if it does not work, we may need to reconnect to the database.
##################################################################################
# If there is an error: Error: FATAL Error: Failed to create checkpoint because of error: INTERNAL Error: Unsupported compression function type
# 1. Update the arrow and duckdb packages: install.packages(c("arrow", "duckdb", "duckdbfs", "duckplyr"))
# 2. Restart R session: Ctrl+ Shift + F10
##################################################################################



#################################################################################
# Read data table: method 1. using dbplyr
#################################################################################


# create / connect to database file in another way
drv <- duckdb(dbdir = file.path(test_csv_folder, "bcstats_db.duckdb"),
              read_only = TRUE) # Only read table not write data.
bcstats_con <- dbConnect(drv)

# Show how many tables in database
dbListTables(bcstats_con)
# many tables.
# list columns/fields in one table
dbListFields(bcstats_con, "BC_Stat_Population_Estimates_20240827")

dbListFields(bcstats_con, "BC_Stat_CLR_EXT_20230525")


# use dplyr syntax
table1 <- dplyr::tbl(bcstats_con, "BC_Stat_Population_Estimates_20240827")
table1 %>%
  select(STUDY_ID, BIRTH_YR_MON,LHA) %>%
  head(6)

# table1 %>%
#   filter()

######################################################################################
# Read data table: method 2. use SQL query directly
########################################################################################

## use SQL

my_query= "SELECT STUDY_ID, BIRTH_YR_MON,LHA FROM BC_Stat_Population_Estimates_20240827"

# Query the table
result <- dbGetQuery(bcstats_con,
                     my_query
)
print(result)


# Query the table
result <- dbGetQuery(bcstats_con, "SELECT * FROM BC_Stat_CLR_EXT_20230525")
print(result)

result %>% readr::write_csv(file = "test_table.csv")

# write_excel_csv()

# or use dbplyr to query the table
BC_Stat_CLR_EXT_20230525 <- dplyr::tbl(bcstats_con, "BC_Stat_CLR_EXT_20230525")

BC_Stat_CLR_EXT_20230525 %>%
  glimpse()

BC_Stat_CLR_EXT_20230525 %>%
  select(BIRTH_YR_MON) %>%
  head()






# do not forget to disconnect from the database
duckdb_shutdown(drv)
dbDisconnect(bcstats_con, shutdown = TRUE)



# or use dbplyr to query the table
# BC_Stat_Population_Estimates_20240527 <- tbl(bcstats_con, "BC_Stat_Population_Estimates_20240527")
#
# BC_Stat_Population_Estimates_20240527 %>%
#   select(POSTAL_CODE) %>%
#   head()
#
# BC_Stat_Population_Estimates_20240527 %>%
#   glimpse()
#
# # or use dplyr to filter the data
# BC_Stat_Population_Estimates_20240527 %>%
#   filter(LHA == "Vancouver Coastal Health") %>%
#   glimpse()
# all dplyr verbs are supported.



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
