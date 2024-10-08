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

pacman::p_load(odbc, tidyverse, config, DBI, dbplyr,nanoarrow, arrow, duckdb,tictoc)

# import functions from utils.r
source(file = "./utils/utils.r")

# Define the path to the test CSV folder
# This path is retrieved from the configuration file
test_csv_folder = config::get("test_sql_server_csv")


##################################################################################
# Test loading a big CSV file to duckdb database directly
##################################################################################


# Create a connection to a new DuckDB database file
# The function will create a duckdb database file in the test_csv_folder if there is no such file
bcstats_con <- duckdb::dbConnect(duckdb::duckdb(),
                                 file.path(test_csv_folder, "db/bcstats_db.duckdb"))

## test connection by write a small table to it
# dbWriteTable(bcstats_con, "iris", iris)

# list all tables in the database
dbListTables(bcstats_con)


# file_path = files_to_process [2]
# table_name = table_names[2]
# con = bcstats_con


# List all CSV and zip files in the test folder
# This will return a character vector of file paths
files_to_process  = list.files(
  file.path(test_csv_folder, "raw_data"),
  pattern = "*.csv|*.zip",
  full.names = TRUE)
# There are tow files that have un utf8 encoding in their string columns. We have to convert them into UTF8 from ascii encoding in github shell.
# -- This command will:
#   -- Attempt to convert from ASCII to UTF-8
# -- Use //TRANSLIT to replace characters that can't be represented in UTF-8
# -- Use -c to skip invalid characters
# iconv -f ASCII -t UTF-8//TRANSLIT -c raw_data/98-401-X2021006_English_CSV_data_BritishColumbia.csv > raw_data/98-401-X2021006_English_CSV_data_BritishColumbia-utf8.csv
# iconv -f ASCII -t UTF-8//TRANSLIT -c raw_data/98-401-X2021025_English_CSV_data.csv > raw_data/98-401-X2021025_English_CSV_data-utf8.csv


# Print the number of CSV files found
cat("Number of CSV files found:", length(files_to_process ), "\n")

# Print the first file path (if any files were found)
if (length(files_to_process ) > 0) {
  cat("First CSV file:", files_to_process [1], "\n")
} else {
  cat("No CSV files found in the specified folder.\n")
}

# retrieve the file name from the file path, which is the table name
table_names = basename(files_to_process ) %>% str_remove(".csv|.zip")
# some table names are not valid in duckdb, we need to change them
table_names = table_names %>% clean_table_names() # clean_table_names is from utils.r
# # Done: clean the table names, roll back one month, rules,




##################################################################################
# First method using the custom function
##################################################################################





log_path <- file.path(test_csv_folder, "logfile.log")


write_log(log_path, "Starting CSV to database import process")



total_tic <- tic("Total processing time")

for (file in files_to_process[1:2]) {
  # table_name <- tools::file_path_sans_ext(basename(file))
  table_name <- basename(file ) %>% str_remove(".csv|.zip") %>% clean_table_names()
  write_log(log_path, paste("Processing file:", file))

  print(paste("Processing file:", file))
  file_tic <- tic(paste("Processing", file))
  tryCatch({
    load_csv_save_db(bcstats_con, file, table_name, log_path)
  }, error = function(e) {
    write_log(log_path, paste("Error processing file", file, ":", e$message))
  })
  file_toc <- toc(log = TRUE, quiet = TRUE)
  write_log(log_path, paste("Total time for", file, ":", round(file_toc$toc - file_toc$tic, 2), "seconds"))
}

total_toc <- toc(log = TRUE, quiet = TRUE)
write_log(log_path, paste("Total processing time:", round(total_toc$toc - total_toc$tic, 2), "seconds"))

DBI::dbDisconnect(bcstats_con)
write_log(log_path, "Finished CSV to database import process")


##################################################################################
# Test reading tables
##################################################################################

test_tab1 <- tbl(bcstats_con, "tab_98_401_X2021025_English_CSV_data_utf8")
test_tab1 %>% head(5) %>% collect()

test_tab1_df <- test_tab1 %>%
  head(654) %>%
  collect()

# even in R, types={'GEO_NAME': 'VARCHAR', 'CHARACTERISTIC_NAME': 'VARCHAR'} are not executed
# Partial Specification
table_data<- read_csv_arrow(files_to_process[1], as_data_frame = F,
                            col_types =schema(GEO_NAME = string(), CHARACTERISTIC_NAME = string()) )

table_data_df = table_data %>%
  head(654) %>%
  collect()

##################################################################################
# If there is an error: Error: FATAL Error: Failed to create checkpoint because of error: INTERNAL Error: Unsupported compression function type
# 1. Update the arrow and duckdb packages: install.packages(c("arrow", "duckdb", "duckdbfs", "duckplyr"))
# 2. Restart R session: Ctrl+ Shift + F10
# 3. Close other applications to get more ram
##################################################################################



##################################################################################
# Second method using the duckdb_read_csv function
# pro: efficient, can handle duplicated column names automatically
##################################################################################
tictoc::tic()
duckdb::duckdb_read_csv(
  conn = bcstats_con,
  name = table_names[2],       # Name of the table to create
  files = files_to_process[2],  # Path to the CSV file
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

# more tests are in '03_analysis.R'
