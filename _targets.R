# Note: the resultant targets have the form `results_{method}_{source}_{csv_id}_{iteration}`
# So, for example, the object called `results_duckDB_LAN_1_2` has the results for using  duckDB, from the LAN, on CSV #1, with iteration #2.

# Also, note that my use of targets here was not a complete success. A lot of that reason is that making and reading databases is finicky. You will see, for example, a few instances in which I create/remove the file db.duckdb -- this is because I got lots of errors trying to just run the loops and do 'normal' things in R.


pacman::p_load(targets, tarchetypes, tidyverse)

# standard set up for a targets project
tar_source()
tar_option_set(packages = c("readr", "tidyverse", "purrr", "duckdb", "DBI"))

# make sure there isn't any residual duckDB file
try(duckdb::dbDisconnect(db, shutdown = TRUE), silent = T)
if (fs::file_exists("db.duckdb")) {
  gc()
  system("rm db.duckdb")
}

params = list(

  n_iterations = 2,

  # a tibble that gives the directory to read from as well as a name (eg LAN or OneDrive)
  csv_paths = tibble::tribble(~dir, ~source,
    "G:/Operations/Data Science and Analytics/2024_bcstats_db/csvs", "LAN",
    "C:/Users/thister/OneDrive - Government of BC/2024-025 Brett and Jon Database Test Warehouse/raw_data", "OneDrive"),

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

# I wanted to make this a target but it was not possible to iterate over it (using `pattern`) and get reasonable names, so I make it a global object instead
the_plan = get_csvs(params$csv_path, params$csvs) |>
  crossing(iteration = 1:params$n_iterations)

saveRDS(the_plan, "RDS/the_plan.Rds")

# we make two targets, one for `read_csv` and the other for duckDB. In each target, we first read the data and then run the queries. For `read_csv` there are two query runs: one with just "regular" dplyr and the other with `duckplyr`.

list(
  tar_map(
    values = the_plan,
    names = c("source", "csv_id", "iteration"),

    tar_target(results_read_csv, {

      # read the dataframe into `df`
      start = Sys.time()
      df = readr::read_csv(path, quote = "\"", progress = F, show_col_types = F)
      end = Sys.time()
      read_time = difftime(end, start)

      # run the queries on df
      query_time = apply(filter(params$query_additions, !use_db), 1, function(row) {
        query = make_query(row[['prefix']], base_query, row[['suffix']])
        start = Sys.time()
        query(df)
        end = Sys.time()
        difftime(end, start)
      })

      return(list(read_time = read_time, query_time = query_time))
    }),

    tar_target(results_duckDB, {

      # just in case the db already exists
      try(duckdb::dbDisconnect(db, shutdown = TRUE), silent = T)
      if (fs::file_exists("db.duckdb")) {
        gc()
        system("rm db.duckdb")
      }

      db = duckdb::dbConnect(duckdb::duckdb(), dbdir = "db.duckdb")

      # attempt to create the duckDB db
      start = Sys.time()
      success = read_data_with_duckDB(csvs$path, nrow.check = 500)
      end = Sys.time()

      # sometimes it doesn't work, so re-run but use `nrow.check = Inf`
      if (!success) {

        # remove the db from previous step
        try(duckdb::dbDisconnect(db, shutdown = TRUE), silent = T)
        if (fs::file_exists("db.duckdb")) {
          gc()
          system("rm db.duckdb")
        }

        # read the db with `nrow.check = Inf`
        db = duckdb::dbConnect(duckdb::duckdb(), dbdir = "db.duckdb")
        start = Sys.time()
        duckdb::duckdb_read_csv(conn = db, name = 't1', files = path, header = T, nrow.check = Inf, transaction = T)
        end = Sys.time()
      }
      read_time = difftime(end, start)

      # run the queries on the duckdb db
      query_time = apply(filter(params$query_additions, use_db), 1, function(row) {
        query = make_query(row[['prefix']], base_query, row[['suffix']])
        start = Sys.time()
        query(db)
        end = Sys.time()
        difftime(end, start)
    })

      duckdb::dbDisconnect(db, shutdown = TRUE)
      gc()
      system("rm db.duckdb")

    return(list(success = success, read_time = read_time, query_time = query_time))
    })
  )
)
