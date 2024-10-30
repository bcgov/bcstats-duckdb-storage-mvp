# Note: the resultant targets either have the form
#
# `read_csv_{source}_{csv_id}_{iteration}`
#
# or
# `duckDB_{db-location}_{source}_{csv_id}_{iteration}`

# So, for example, the object called `duckDB_OneDrive_LAN_1_2` stores the results for making a duckDB database that's saved on OneDrive, populated from csv files from the LAN, using CSV #1, and iteration #2. (Phew!)

# Also, note that my use of targets here was not a complete success. A lot of that reason is that making and reading databases is finicky. You will see, for example, a few instances in which I create/remove the file db.duckdb -- this is because I got lots of errors trying to just run the loops and do 'normal' things in R.

# run this if you want to copy the _targets folder over to onedrive
# fs::dir_copy("_targets", params$paths[['OneDrive']], overwrite = T)

pacman::p_load(targets, tarchetypes, tidyverse)

# standard set up for a targets project
tar_source()
tar_option_set(packages = c("readr", "tidyverse", "purrr", "duckdb", "DBI"))

params = list(

  n_iterations = 2,

  paths = c(
    LAN = "G:/Operations/Data Science and Analytics/2024_bcstats_db",
    OneDrive = "C:/Users/thister/OneDrive - Government of BC/2024-025 Brett and Jon Database Test Warehouse"
  ),

  # a tibble with the CSVs to read as well as a query to run on them to time
  csvs = tibble::tribble(~csv, ~base_query,
      "98-401-X2021006_English_CSV_data_BritishColumbia-utf8.csv", "mutate(C1_COUNT_TOTAL = as.double(C1_COUNT_TOTAL)) |> group_by(CENSUS_YEAR, GEO_LEVEL, GEO_NAME) |> summarise(C1_COUNT_TOTAL = sum(C1_COUNT_TOTAL), .groups='drop') |> arrange(CENSUS_YEAR, GEO_LEVEL, GEO_NAME)",

      "BC Stat Population Estimates_20240429.csv", "count(CITY, SEX) |> arrange(CITY, SEX)",

     "CLR_EXT_20231227.csv", "count(CITY) |> arrange(CITY)",

     "bca_folio_gnrl_property_values_20240825_WEEKLY.csv", "summarise(across(everything(), min))",

     "bca_folio_sales_20240825_WEEKLY.csv", "count(JURISDICTION, CONVEYANCE_TYPE_DESCRIPTION) |> arrange(JURISDICTION, CONVEYANCE_TYPE_DESCRIPTION)"
  ),

  # this tibble gets recycled over the base queries defined above
  query_additions = tibble::tibble(
    prefix = c(
      "function(df) df",
      "function(df) df |> duckplyr::as_duckplyr_tibble()",
      "function(conn) tbl(conn, 't1')"
    ),
    suffix = c(
      "as_tibble()",
      "as_tibble()",
      "collect()"
    ),
    use_db = c(F, F, T) # a flag whether the query uses duckDB
  )
)

# I wanted to make this a target but it was not possible to iterate over it (using `pattern`) and get reasonable names for the targets, so I make it a global object instead
the_plan = get_csvs(params$csv_path, params$csvs) |>
  crossing(iteration = 1:params$n_iterations)

saveRDS(the_plan, "RDS/the_plan.Rds")







# we make two targets, one for `read_csv` and the other for duckDB. In each target, we first read the data and then run the queries. For `read_csv` there are two query runs: one with just "regular" dplyr and the other with `duckplyr`.

list(
  tar_map(
    values = the_plan,
    names = c("source", "csv_id", "iteration"),

    tar_target(read_csv, read_csv_target(path = path, base_query = base_query)),

    tar_target(duckDB_local, duckDB_target(dbdir = "speedtest_db.duckdb", path = path, base_query = base_query)),

    tar_target(duckDB_LAN, duckDB_target(dbdir = paste0(params$paths[['LAN']], "/db/speedtest_db.duckdb"), path = path, base_query = base_query)),

    tar_target(duckDB_OneDrive, duckDB_target(dbdir = paste0(params$paths[['OneDrive']], "/db/speedtest_db.duckdb"), path = path, base_query = base_query))

  )
)
