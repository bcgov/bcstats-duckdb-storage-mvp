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
# load packages
pacman::p_load(odbc, tidyverse, config, DBI, dbplyr,nanoarrow, arrow, duckdb)


# setwd("path")

# Define the path to the test CSV folder
# This path is retrieved from the configuration file
test_csv_folder = config::get("test_sql_server_csv")



# List all CSV files in the test folder
# This will return a character vector of file paths
files_to_process  = list.files(
  file.path(test_csv_folder, "raw_data"),
  pattern = "*.csv|*.zip",
  full.names = TRUE)

# Print the number of CSV files found
cat("Number of CSV files found:", length(files_to_process ), "\n")

# Print the first file path (if any files were found)
if (length(files_to_process ) > 0) {
  cat("First CSV file:", files_to_process [1], "\n")
} else {
  cat("No CSV files found in the specified folder.\n")
}

# retrieve the file name from the file path, which is the table name
table_names = basename(files_to_process ) %>% str_remove(".csv")
# some table names are not valid in database, we need to change them
table_names = table_names %>% clean_table_names() # clean_table_names is from utils.r
# # Done: clean the table names, roll back one month, rules,


##################################################################################
# Test loading a big file to MS sql server
##################################################################################

# Connect to SQL Server
ms_sql_con <-dbConnect(odbc::odbc(),
                Driver = config::get("Driver"),
                Server = config::get("Server"),
                Database = config::get("Database"),
                Trusted_Connection = "True"
)

# Build a vector of table names: tables
tables <- dbListTables(ms_sql_con)


# tables have the all table names
dbGetInfo(ms_sql_con)
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


# import functions from utils.r
source(file = "./utils/utils.r")


# why it works outside the function?
table_data = read_csv_arrow(files_to_process[12])

nanoarrow_stream <- nanoarrow::as_nanoarrow_array_stream(table_data)

table_name = "GCS_202406"
if (DBI::dbExistsTable(ms_sql_con, table_name)) {
  # write_log(log_path, paste("Dropping existing table:", table_name))
  DBI::dbRemoveTable(ms_sql_con, table_name)
}

tic("Writing to database")

DBI::dbWriteTableArrow(
  ms_sql_con,
  name = table_name,
  nanoarrow_stream,
  overwrite = TRUE
)

# this one simple table works, and other tables work in duckdb, but do not work in ms sql server. And error messages were not clear.
# errors are similar to Bret's error
#
# [Microsoft][ODBC SQL Server Driver][DBNETLIB]ConnectionWrite (send()).
# [Microsoft][ODBC SQL Server Driver][DBNETLIB]General network error. Check your network documentation.

##################################################################################
# Test loading all big files in raw_data folder to MS sql server
##################################################################################


log_path <- file.path(test_csv_folder, "logfile_ms_sql.log")


write_log(log_path, "Starting CSV to database import process")



total_tic <- tic("Total processing time")

for (file in files_to_process[3:12]) {
  # table_name <- tools::file_path_sans_ext(basename(file))
  table_name <- basename(file ) %>% str_remove(".csv|.zip") %>% clean_table_names()
  write_log(log_path, paste("Processing file:", file))

  print(paste("Processing file:", file))
  file_tic <- tic(paste("Processing", file))
  tryCatch({
    load_csv_save_db(ms_sql_con, file, table_name, log_path)
  }, error = function(e) {
    write_log(log_path, paste("Error processing file", file, ":", e$message))
  })
  file_toc <- toc(log = TRUE, quiet = TRUE)
  write_log(log_path, paste("Total time for", file, ":", round(file_toc$toc - file_toc$tic, 2), "seconds"))
}

total_toc <- toc(log = TRUE, quiet = TRUE)
write_log(log_path, paste("Total processing time:", round(total_toc$toc - total_toc$tic, 2), "seconds"))

DBI::dbDisconnect(ms_sql_con)
write_log(log_path, "Finished CSV to database import process")


############################################################################################



# as long as we know the csv file path, we can load them in arrow and save arrow object to sql server.
load_csv_save_db(con,
                 file_path = files_to_process [2],
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
                 file_path = files_to_process [3],
                 table_name = table_names[3])


# New code should prefer dbCreateTable() and dbAppendTable().

#################################################################################
# list tables and list fields in tables
#################################################################################
# sql_translate_env(con)
# dbListObjects(con)

# list tables
dbListTables(con)

# list fields in table
dbListFields(
  conn = con,
  name = "FCT_TMF_202312"
)

#################################################################################
# Read data table
#################################################################################





# this is the decimal database schema that Brett created. All the existed tables are under this schema.
table_prefix = config::get("table_prefix")
# this is the schema that the author is going to use
# table_prefix_new = config::get("table_prefix_new")


print("Sepecify the table name")
tbl_name = "FCT_CENSUS_2021_BC_DA"
print("Put your query in a string")


# FCT_TMF_202312_tbl = tbl(con, "[IDRI\\BDWILMER].FCT_TMF_202312")

FCT_TMF_202312_tbl = read_sql_query(con, tbl_name, table_prefix)
FCT_TMF_202312_tbl %>% glimpse()




my_query = "with first_year as
(
select distinct LHA as FY_LHA, study_id as FY_study_id
from [IDIR\JDUAN].[BC_Stat_CLR_EXT_202304]
),

second_year as
(
select distinct LHA as SY_LHA, study_id as SY_study_id
from [IDIR\JDUAN].[BC_Stat_Population_Estimates_202404]
where end_date>='2024-04-01' or end_date is null
),

total_pop as
(
select LHA as SY_LHA,
count(distinct(study_id)) as SY_study_id
from [IDIR\JDUAN].[BC_Stat_Population_Estimates_202404]
where end_date>='2024-04-01' or end_date is null
group by LHA
),

first_pop as
(
select LHA as SY_LHA,
count(distinct(study_id)) as FY_study_id
from [IDIR\JDUAN].[BC_Stat_CLR_EXT_202304]
--where end_date>='2023-07-01' or end_date is null
group by LHA
),

all_movements as
(
select
FY_LHA,SY_LHA,
case
--when FY_study_id is null then 'ENTERED'
--when SY_study_id is null then 'EXITED'
when FY_LHA is not null and SY_LHA is null then 'EXITED'
when FY_LHA is null and SY_LHA is not null then 'ENTERED'
when FY_LHA is null and SY_LHA is null then 'EXISTING'
when FY_LHA<>SY_LHA then 'MOVED'
when FY_LHA=SY_LHA then 'EXISTING'
else 'ERROR' end as STATUS,
count (distinct(FY_study_id)) as FY_study_id,
count (distinct(SY_study_id)) as SY_study_id
from first_year full outer join second_year on FY_study_id=SY_study_id
group by FY_LHA,SY_LHA,
case
--when FY_study_id is null then 'ENTERED'
--when SY_study_id is null then 'EXITED'
when FY_LHA is not null and SY_LHA is null then 'EXITED'
when FY_LHA is null and SY_LHA is not null then 'ENTERED'
when FY_LHA is null and SY_LHA is null then 'EXISTING'
when FY_LHA<>SY_LHA then 'MOVED'
when FY_LHA=SY_LHA then 'EXISTING'
else 'ERROR' end
),

entered as
(
select * from all_movements
where status= 'ENTERED'
),

exited as
(
select * from all_movements
where status= 'EXITED'
),

moved as
(
select * from all_movements
where status= 'MOVED'
)

--all movements
--select * from all_movements;

--final table of immigration inflows and outflows
select
b.FY_LHA,
--a.status,b.status,
a. SY_study_id as entered,
b.FY_study_id as exited,
(a. SY_study_id-b.FY_study_id) as net_growth,
d.FY_study_id as begin_pop,
c.SY_study_id as end_pop
from entered a
join exited b on b.FY_LHA = a.SY_LHA
join total_pop c on c.SY_LHA = a.SY_LHA
join first_pop d on d.SY_LHA = a.SY_LHA
order by b.FY_LHA

;
"




# Notes:

# Based on the search results, using `nanoarrow::as_nanoarrow_array_stream(data)` with `dbWriteTableArrow()` is generally preferred over directly using `dbWriteTable()` for a few key reasons:

# 1. Performance: The Arrow-based approach can be significantly faster, especially for larger datasets. This is because it leverages Arrow's efficient columnar format and avoids unnecessary data conversions [christophenicault.com](https://www.christophenicault.com/post/large_dataframe_arrow_duckdb/).

# 2. Memory efficiency: When using Arrow, the data doesn't need to be fully loaded into R's memory. This is particularly beneficial for large datasets that might not fit into RAM [arrow.apache.org](https://arrow.apache.org/docs/r/articles/data_wrangling.html).

# 3. Type fidelity: Arrow maintains better type fidelity between R and the database, which can be important for certain data types or when working with multiple languages or systems [cran.r-project.org](https://cran.r-project.org/web/packages/DBI/vignettes/DBI-arrow.html).

# 4. Interoperability: The Arrow format is designed for cross-language compatibility, making it easier to work with the same data in different environments (R, Python, C++, etc.) [arrow.apache.org](https://arrow.apache.org/docs/r/).

# 5. Lazy evaluation: Arrow uses lazy evaluation, which can optimize query execution by allowing the database to perform multiple computations in one operation [christophenicault.com](https://www.christophenicault.com/post/large_dataframe_arrow_duckdb/).

# The `nanoarrow::as_nanoarrow_array_stream()` function specifically converts the data into an Arrow array stream, which is a more efficient format for transferring data to the database when using `dbWriteTableArrow()`.

# It's worth noting that while this approach is generally more efficient, the best method can depend on your specific use case, data size, and the capabilities of the database you're working with. For smaller datasets or simpler operations, the performance difference might be negligible.

