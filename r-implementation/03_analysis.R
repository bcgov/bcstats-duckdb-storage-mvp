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

# Load the DuckDB package
library(duckdb)

# Connect to the DuckDB database
con <- dbConnect(duckdb::duckdb(),
                 file.path(test_csv_folder, "db/bcstats_db_dev.duckdb"))

# Fetch the list of tables with 'stg%' prefix
stg_tables <- dbGetQuery(con, "
    SELECT table_name
    FROM information_schema.tables
    WHERE table_name LIKE 'stg%'
")

# Iterate over the result and drop each table
for (table_name in stg_tables$table_name) {
  drop_sql <- paste0("DROP TABLE IF EXISTS ", table_name, ";")
  print(paste("Executing:", drop_sql))
  dbExecute(con, drop_sql)  # Execute the drop statement
}

# Disconnect from the DuckDB database
dbDisconnect(con, shutdown = TRUE)

#####################################################################


# create / connect to database file in another way
drv <- duckdb(dbdir = file.path(test_csv_folder, "db/bcstats_db.duckdb"),
              read_only = TRUE) # Only read table not write data.
bcstats_read_con <- dbConnect(drv)

# Show how many tables in database
all_tbl_list = dbListTables(bcstats_read_con)
# many tables.
# list columns/fields in one table

dbListFields(bcstats_read_con, "stg_Census_2021_PUMF_data_donnees_2021_ind_v2")

dbListFields(bcstats_read_con, "BC_Stat_CLR_EXT_20230525")


# do not forget to disconnect from the database
duckdb_shutdown(drv)
dbDisconnect(bcstats_read_con, shutdown = TRUE)






######################################################################################
# Read data table: method 1. dbplyr
########################################################################################

tbl_98_401_X2021006_English_BC_db = tbl(bcstats_read_con, "tbl_98_401_X2021006_English_BC")


tbl_98_401_X2021006_English_BC_db %>%
  head(4) %>%
  collect()


tbl_98_401_X2021025_English_db = tbl(bcstats_read_con, "tbl_98_401_X2021025_English")


tbl_98_401_X2021025_English_db %>%
  head(4) %>%
  collect()

######################################################################################
# Read data table: method 2. use SQL query directly
########################################################################################

## use SQL

# my_query= "SELECT STUDY_ID, BIRTH_YR_MON,LHA FROM BC_Stat_Population_Estimates_20240827"
#
# # Query the table
# result <- dbGetQuery(bcstats_con,
#                      my_query
# )
# print(result)
#
#
# # Query the table
# result <- dbGetQuery(bcstats_con, "SELECT * FROM BC_Stat_CLR_EXT_20230525")
# print(result)
#
# result %>% readr::write_csv(file = "test_table.csv")
#
# # write_excel_csv()
#
# # or use dbplyr to query the table
# BC_Stat_CLR_EXT_20230525 <- dplyr::tbl(bcstats_con, "BC_Stat_CLR_EXT_20230525")
#
# BC_Stat_CLR_EXT_20230525 %>%
#   glimpse()
#
# BC_Stat_CLR_EXT_20230525 %>%
#   select(BIRTH_YR_MON) %>%
#   head()


# the following code may not run, then highlight the whole statement, and ctrl + enter
sql_query <- "
with first_year as
(
select distinct LHA as FY_LHA, STUDY_ID as FY_STUDY_ID
from BC_Stat_CLR_EXT_20230525
),

second_year as
(
select distinct LHA as SY_LHA, STUDY_ID as SY_STUDY_ID
from BC_Stat_Population_Estimates_20240527
where end_date>='2024-04-01' or end_date is null
),

total_pop as
(
select LHA as SY_LHA,
count(distinct(STUDY_ID)) as SY_STUDY_ID
from BC_Stat_Population_Estimates_20240527
where end_date>='2024-04-01' or end_date is null
group by LHA
),

first_pop as
(
select LHA as SY_LHA,
count(distinct(STUDY_ID)) as FY_STUDY_ID
from BC_Stat_CLR_EXT_20230525
--where end_date>='2023-07-01' or end_date is null
group by LHA
),

all_movements as
(
select
FY_LHA,SY_LHA,
case

when FY_LHA is not null and SY_LHA is null then 'EXITED'
when FY_LHA is null and SY_LHA is not null then 'ENTERED'
when FY_LHA is null and SY_LHA is null then 'EXISTING'
when FY_LHA<>SY_LHA then 'MOVED'
when FY_LHA=SY_LHA then 'EXISTING'
else 'ERROR' end as STATUS,
count (distinct(FY_STUDY_ID)) as FY_STUDY_ID,
count (distinct(SY_STUDY_ID)) as SY_STUDY_ID
from first_year full outer join second_year on FY_STUDY_ID=SY_STUDY_ID
group by FY_LHA,SY_LHA,
case
--when FY_STUDY_ID is null then 'ENTERED'
--when SY_STUDY_ID is null then 'EXITED'
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
a. SY_STUDY_ID as entered,
b.FY_STUDY_ID as exited,
(a. SY_STUDY_ID-b.FY_STUDY_ID) as net_growth,
d.FY_STUDY_ID as begin_pop,
c.SY_STUDY_ID as end_pop
from entered a
join exited b on b.FY_LHA = a.SY_LHA
join total_pop c on c.SY_LHA = a.SY_LHA
join first_pop d on d.SY_LHA = a.SY_LHA
order by b.FY_LHA
"

# Use dbGetQuery() for simple queries or dbSendQuery() followed by dbFetch() for more control over large result sets.
tictoc::tic()
result <- dbGetQuery(bcstats_read_con, sql_query)
tictoc::toc()
# 18.61 sec elapsed
result %>% glimpse()

# or
tictoc::tic()
result <- dbSendQuery(bcstats_read_con, sql_query)
data <- dbFetch(result)
dbClearResult(result)
tictoc::toc()
# 6.84 sec elapsed


# Use raw string literals (for R 4.0+):
sql_query <- R"(
with first_year as
(
select distinct LHA as FY_LHA, STUDY_ID as FY_STUDY_ID
from BC_Stat_CLR_EXT_20230525
),

second_year as
(
select distinct LHA as SY_LHA, STUDY_ID as SY_STUDY_ID
from BC_Stat_Population_Estimates_20240527
where end_date>='2024-04-01' or end_date is null
),

total_pop as
(
select LHA as SY_LHA,
count(distinct(STUDY_ID)) as SY_STUDY_ID
from BC_Stat_Population_Estimates_20240527
where end_date>='2024-04-01' or end_date is null
group by LHA
),

first_pop as
(
select LHA as SY_LHA,
count(distinct(STUDY_ID)) as FY_STUDY_ID
from BC_Stat_CLR_EXT_20230525
--where end_date>='2023-07-01' or end_date is null
group by LHA
),

all_movements as
(
select
FY_LHA,SY_LHA,
case

when FY_LHA is not null and SY_LHA is null then 'EXITED'
when FY_LHA is null and SY_LHA is not null then 'ENTERED'
when FY_LHA is null and SY_LHA is null then 'EXISTING'
when FY_LHA<>SY_LHA then 'MOVED'
when FY_LHA=SY_LHA then 'EXISTING'
else 'ERROR' end as STATUS,
count (distinct(FY_STUDY_ID)) as FY_STUDY_ID,
count (distinct(SY_STUDY_ID)) as SY_STUDY_ID
from first_year full outer join second_year on FY_STUDY_ID=SY_STUDY_ID
group by FY_LHA,SY_LHA,
case
--when FY_STUDY_ID is null then 'ENTERED'
--when SY_STUDY_ID is null then 'EXITED'
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
a. SY_STUDY_ID as entered,
b.FY_STUDY_ID as exited,
(a. SY_STUDY_ID-b.FY_STUDY_ID) as net_growth,
d.FY_STUDY_ID as begin_pop,
c.SY_STUDY_ID as end_pop
from entered a
join exited b on b.FY_LHA = a.SY_LHA
join total_pop c on c.SY_LHA = a.SY_LHA
join first_pop d on d.SY_LHA = a.SY_LHA
order by b.FY_LHA
)"

tictoc::tic()
result <- dbGetQuery(bcstats_read_con, sql_query)
tictoc::toc()
# 18.61 sec elapsed
result %>% glimpse()

# or
tictoc::tic()
result <- dbSendQuery(bcstats_read_con, sql_query)
data <- dbFetch(result)
dbClearResult(result)
tictoc::toc()

######################################################################################
# Read data table: method 3. read SQL files and query in R
########################################################################################

# second method
# Keep SQL queries in separate .sql files and read them in:


# tbl(src = bcstats_read_con, sql())
sql_query <- readLines("test_sql.sql") %>% paste(collapse = "\n") %>% glue::glue_sql_collapse()
sql_query <- read_file("test_sql.sql")%>% glue::glue_sql_collapse()

tictoc::tic()
result <- dbGetQuery(bcstats_read_con, sql_query)
tictoc::toc()
# 18.61 sec elapsed
result %>% glimpse()



# or
tictoc::tic()
result <- dbSendQuery(bcstats_read_con, sql_query)
data <- dbFetch(result)
dbClearResult(result)
tictoc::toc()



######################################################################################
# Read data table: method 4. parameterized query
########################################################################################


# For dynamic queries, use parameterized queries with sqlInterpolate() to prevent SQL injection.
library(DBI)
con <- dbConnect(duckdb::duckdb())

sql_template <- "
  SELECT *
  FROM my_table
  WHERE column1 = ?value
    AND column2 > ?threshold
  ORDER BY column3 DESC
"

sql_query <- sqlInterpolate(con, sql_template,
                            value = "some_value",
                            threshold = 100)

# do not forget to disconnect from the database
duckdb_shutdown(drv)
dbDisconnect(bcstats_read_con, shutdown = TRUE)




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
