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

pacman::p_load(odbc, tidyverse, DBI, dbplyr,nanoarrow, arrow)


##################################################################################
# Test loading a big file to MS sql server
##################################################################################

con <-dbConnect(odbc(),
                Driver = config::get("Driver"),
                Server = config::get("Server"),
                Database = config::get("Database"),
                # UID = "JDUAN",
                Trusted_Connection = "True"
)


# read data
# data <- open_dataset(sources = flnm,
#                      format = format,
#                      schema = schema,
#                      skip = 1)


test_csv_folder = config::get("test_sql_server_csv")
list.files(test_csv_folder, pattern = "*.csv", full.names = T)[1]
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

##################################################################################
# Test loading a big file to duckdb
##################################################################################
# write to arrow dataset
tictoc::tic()
write_dataset(dataset =  data %>% group_by(LHA),
              path = paste0(test_csv_folder, "/BC_Stat_Population_Estimates_20240527"),
              format = "parquet"
)
tictoc::toc()
# 6.2 sec elapsed

#################################################################################
#################################################################################
# second dataset
data <- read_csv_arrow(file = list.files(test_csv_folder,pattern = "*.csv", full.names = T)[2])
data %>% glimpse()


# write to sql server
tictoc::tic()
DBI::dbWriteTableArrow(con,
                       name = "BC_Stat_CLR_EXT_20230525",
                       nanoarrow::as_nanoarrow_array_stream(data)
                       # append = append
)

tictoc::toc()
# 412.19 sec elapsed

##################################################################################
# Test loading a big file to duckdb
##################################################################################
# write to arrow dataset
tictoc::tic()
write_dataset(dataset =  data %>% group_by(LHA),
              path = paste0(test_csv_folder, "/BC_Stat_CLR_EXT_20230525"),
              format = "parquet"
)
tictoc::toc()
# 3.08 sec elapsed



##################################################################################
# Using duckdb to join data
##################################################################################



BC_Stat_Population_Estimates_20240527 = open_dataset(paste0(test_csv_folder, "/BC_Stat_Population_Estimates_20240527"))

BC_Stat_CLR_EXT_20230525 = open_dataset(paste0(test_csv_folder, "/BC_Stat_CLR_EXT_20230525"))

BC_Stat_Population_Estimates_20240527 %>%
  glimpse()

BC_Stat_CLR_EXT_20230525 %>%
  glimpse()


##################################################################################
# Test loading a big file to duckdb database
##################################################################################


# Create a connection to a new DuckDB database file
bcstats_con <- dbConnect(duckdb::duckdb(), paste0(test_csv_folder, "/bcstats_db.duckdb"))

# Load a CSV file into DuckDB
tictoc::tic()
duckdb::duckdb_read_csv(
  conn = bcstats_con,
  name = "BC_Stat_Population_Estimates_20240527",       # Name of the table to create
  files = list.files(test_csv_folder, pattern = "*.csv",full.names = T)[1],  # Path to the CSV file
  header = TRUE,           # Whether the CSV file has a header row
  delim = ",",             # Delimiter used in the CSV file
  quote = "\"",            # Quote character used in the CSV file
  na.strings = "",         # Strings to interpret as NA
  transaction = TRUE       # Whether to wrap the operation in a transaction
)
tictoc::toc()
# 10.61 sec elapsed


# Query the table
result <- dbGetQuery(bcstats_con, "SELECT * FROM BC_Stat_Population_Estimates_20240527 ")
print(result)



# Load a CSV file into DuckDB
tictoc::tic()
duckdb::duckdb_read_csv(
  conn = bcstats_con,
  name = "BC_Stat_CLR_EXT_20230525",       # Name of the table to create
  files = list.files(test_csv_folder, pattern = "*.csv",full.names = T)[2],  # Path to the CSV file
  header = TRUE,           # Whether the CSV file has a header row
  delim = ",",             # Delimiter used in the CSV file
  quote = "\"",            # Quote character used in the CSV file
  na.strings = "",         # Strings to interpret as NA
  transaction = TRUE       # Whether to wrap the operation in a transaction
)
tictoc::toc()
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

dbWriteTable(con, name = "test_mtcars", value = mtcars )

