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

# retrieve the file name from the file path, which is the table name
table_names = basename(csv_files) %>% str_remove(".csv")
# some table names are not valid in database, we need to change them
table_names = table_names %>% str_replace_all(" ", "_")

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
# data <- read_csv_arrow(file = csv_files[1])
#
# # show the data structure
# data %>% glimpse()
#
# # write to sql server
#
# # use tictoc to time the process
# tictoc::tic()
# DBI::dbWriteTableArrow(con,
#                        name = table_names[1],
#                        nanoarrow::as_nanoarrow_array_stream(data)
#                        # append = append
# )
#
# tictoc::toc()
# # 590.52 sec elapsed
#
# # disconnect from the database
# dbDisconnect(con)

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
                         nanoarrow::as_nanoarrow_array_stream(data) # explain why we use nanoarrow here instead of directly using data with dbWriteTable()
# Based on the search results, using `nanoarrow::as_nanoarrow_array_stream(data)` with `dbWriteTableArrow()` is generally preferred over directly using `dbWriteTable()` for a few key reasons:

# 1. Performance: The Arrow-based approach can be significantly faster, especially for larger datasets. This is because it leverages Arrow's efficient columnar format and avoids unnecessary data conversions [christophenicault.com](https://www.christophenicault.com/post/large_dataframe_arrow_duckdb/).

# 2. Memory efficiency: When using Arrow, the data doesn't need to be fully loaded into R's memory. This is particularly beneficial for large datasets that might not fit into RAM [arrow.apache.org](https://arrow.apache.org/docs/r/articles/data_wrangling.html).

# 3. Type fidelity: Arrow maintains better type fidelity between R and the database, which can be important for certain data types or when working with multiple languages or systems [cran.r-project.org](https://cran.r-project.org/web/packages/DBI/vignettes/DBI-arrow.html).

# 4. Interoperability: The Arrow format is designed for cross-language compatibility, making it easier to work with the same data in different environments (R, Python, C++, etc.) [arrow.apache.org](https://arrow.apache.org/docs/r/).

# 5. Lazy evaluation: Arrow uses lazy evaluation, which can optimize query execution by allowing the database to perform multiple computations in one operation [christophenicault.com](https://www.christophenicault.com/post/large_dataframe_arrow_duckdb/).

# The `nanoarrow::as_nanoarrow_array_stream()` function specifically converts the data into an Arrow array stream, which is a more efficient format for transferring data to the database when using `dbWriteTableArrow()`.

# It's worth noting that while this approach is generally more efficient, the best method can depend on your specific use case, data size, and the capabilities of the database you're working with. For smaller datasets or simpler operations, the performance difference might be negligible.

  )
  tictoc::toc()
  # disconnect from the database
  DBI::dbDisconnect(con)
}


# as long as we know the csv file path, we can load them in arrow and save arrow object to sql server.
load_csv_save_db(con,
                 file_path = csv_files[2],
                 table_name = table_names[2])


#################################################################################
#################################################################################
# second dataset

con <-dbConnect(odbc::odbc(),
                Driver = config::get("Driver"),
                Server = config::get("Server"),
                Database = config::get("Database"),
                Trusted_Connection = "True"
)
# 412.19 sec elapsed
load_csv_save_db(con,
                 file_path = csv_files[3],
                 table_name = table_names[3])


# New code should prefer dbCreateTable() and dbAppendTable().


