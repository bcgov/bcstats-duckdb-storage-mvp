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
source("./R/functions.r")



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


union_all_query = "SELECT * FROM (
    SELECT TOP 1000 * FROM CLR_EXT_20200213_for_201107
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20160927

    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20161027
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20161128
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20161229
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20170126
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20170227
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20170324
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20170427
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20170626
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20170727
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20170825
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20170928
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20171026
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20171128
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20171228
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20180228
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20180327
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20180426
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20180529
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20180628
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20180829
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20181129
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20181224
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20190128
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20190228
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20190327
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20190429
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20190628
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20190729
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20190829
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20180730
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20190927
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20191028
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20191128
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20191224
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20200127
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20200227
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20200330
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20200427
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20200525
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20200629
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20200729
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20200825
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20201130
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20201229
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20210128
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20210225
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20210430
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20210609
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20210628
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20210729
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20210830
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20210927
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20211028
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20211129
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20211230
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20220128
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20220228
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20220330
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20220430
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20220530
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20220627
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20220728
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20220829
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20220927
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_202201028
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_202201129
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20230103
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20230227
    UNION ALL
    SELECT TOP 1000 * FROM BC_STAT_POPULATION_ESTIMATES_20230327
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20230426
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20230525
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20230626
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20230726
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20230828
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20230927
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20231030
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20231127
    UNION ALL
    SELECT TOP 1000 * FROM CLR_EXT_20231227
    UNION ALL
    SELECT TOP 1000 * FROM BC_STAT_POPULATION_ESTIMATES_20240129
    UNION ALL
    SELECT TOP 1000 * FROM BC_STAT_POPULATION_ESTIMATES_20240226
    UNION ALL
    SELECT TOP 1000 * FROM BC_STAT_POPULATION_ESTIMATES_20240326
    UNION ALL
    SELECT TOP 1000 * FROM BC_STAT_POPULATION_ESTIMATES_20240429
    UNION ALL
    SELECT TOP 1000 * FROM BC_STAT_POPULATION_ESTIMATES_20240527
    UNION ALL
    SELECT TOP 1000 * FROM BC_STAT_POPULATION_ESTIMATES_20240628
    UNION ALL
    SELECT TOP 1000 * FROM BC_STAT_POPULATION_ESTIMATES_20240726
    UNION ALL
    SELECT TOP 1000 * FROM BC_STAT_POPULATION_ESTIMATES_20240827
    UNION ALL
    SELECT TOP 1000 * FROM bc_stat_population_estimates_20240926
) AS CombinedTables;"
