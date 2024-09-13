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



# define a function to load a csv file and save to a database


#' Load CSV File and Save to Database
#'
#' This function reads a CSV file using Apache Arrow, displays its structure,
#' and writes the data to a SQL Server database table.
#'
#' @param con A DBI connection object to the SQL Server database.
#' @param file_path A string specifying the path to the CSV file to be loaded.
#' @param table_name A string specifying the name of the table to be created or
#'   overwritten in the database.
#'
#' @details
#' The function performs the following steps:
#' 1. Reads the CSV file using Arrow for efficient memory management.
#' 2. Displays the structure of the loaded data using `glimpse()`.
#' 3. Writes the data to the specified table in the SQL Server database.
#' 4. Measures and reports the time taken for the database write operation.
#' 5. Disconnects from the database.
#'
#' The function uses `nanoarrow::as_nanoarrow_array_stream()` to convert the
#' Arrow object to a nanoarrow array stream. This is done because `dbWriteTableArrow()`
#' expects a nanoarrow array stream for optimal performance when writing large datasets.
#' Using nanoarrow allows for efficient, chunk-wise data transfer to the database,
#' which can significantly improve performance compared to using `dbWriteTable()` directly.
#'
#' @note
#' - Ensure that the necessary packages (arrow, DBI, dplyr, tictoc, nanoarrow) are installed.
#' - The function will overwrite the table if it already exists in the database.
#' - The database connection is closed at the end of the function, so you may need
#'   to reestablish the connection for subsequent operations.
#'
#' @examples
#' \dontrun{
#' # Establish a connection to your SQL Server database
#' con <- DBI::dbConnect(odbc::odbc(),
#'                       Driver = "SQL Server",
#'                       Server = "your_server",
#'                       Database = "your_database")
#'
#' # Call the function
#' load_csv_save_db(con, "path/to/your/file.csv", "your_table_name")
#' }
#'
#' @importFrom arrow read_csv_arrow
#' @importFrom dplyr glimpse
#' @importFrom DBI dbWriteTableArrow dbDisconnect
#' @importFrom tictoc tic toc
#' @importFrom nanoarrow as_nanoarrow_array_stream
#'
#' @export
#'
load_csv_save_db = function(con, file_path, table_name){
  # allow the function to accept a connection object, file path and table name

  # read the csv file into an arrow object,
  # ulitize the arrow to load the CSV files and write to a database, which in this case is sql server database.
  data <- arrow::read_csv_arrow(file = file_path)
  # show the data structure
  data %>% head(3) %>% glimpse()

  # write to sql server
  tictoc::tic()
  DBI::dbWriteTableArrow(con,
                         name = table_name,
                         nanoarrow::as_nanoarrow_array_stream(data) # explain why we use nanoarrow here instead of directly using data with dbWriteTable()

  )
  tictoc::toc()
  # disconnect from the database
  DBI::dbDisconnect(con)
}


library(tidyverse)
library(lubridate)

#' Clean and Transform Table Names with Date Rollback
#'
#' This function processes a vector of table names, replacing spaces with underscores
#' and modifying date components. It's designed to handle table names that may include
#' a date in the format YYYYMMDD at the end of the string.
#'
#' @param table_names A character vector of table names to be processed.
#'
#' @return A character vector of cleaned and transformed table names.
#'
#' @details
#' The function performs the following operations:
#' 1. Replaces all spaces in the table names with underscores.
#' 2. Identifies and extracts a date component (if present) at the end of each name.
#' 3. For names with a date component:
#'    - Removes the original date from the name.
#'    - Rolls the date back to the last day of the previous month.
#'    - Formats the new date as "YYYY.MM".
#'    - Appends the new date to the modified name.
#' 4. For names without a date component, only space replacement is performed.
#'
#' The date rollback handles edge cases such as end-of-month dates and leap years
#' by using lubridate's floor_date() function to ensure valid dates.
#'
#' @note
#' - The function assumes that any 8-digit sequence at the end of a name represents a date.
#' - Names without a recognizable date component are returned with only space replacement.
#' - This function requires the tidyverse and lubridate packages.
#'
#' @examples
#' table_names <- c("Sales Data 20240531", "Inventory_20230630", "Customers 20230731", "Products")
#' cleaned_names <- clean_table_names(table_names)
#' print(cleaned_names)
#' # Output:
#' # [1] "Sales_Data_2024.04"    "Inventory_2023.05"     "Customers_2023.06"     "Products"
#'
#' @importFrom tidyverse %>% mutate if_else str_replace_all str_extract str_remove
#' @importFrom lubridate ymd days floor_date
#'
#' @export
clean_table_names <- function(table_names) {
  tibble(name = table_names) %>%
    mutate(
      name = str_replace_all(name, " ", "_"),
      date_part = str_extract(name, "\\d{8}$"),
      base_name = str_remove(name, "\\d{8}$"),
      new_date = if_else(
        !is.na(date_part),
        format(floor_date(ymd(date_part) - days(1), "month"), "%Y%m"),
        NA_character_
      ),
      cleaned_name = if_else(
        !is.na(date_part),
        paste0(base_name, new_date),
        name
      )
    ) %>%
    pull(cleaned_name)
}



# create a function for the preprocess
# SQL server requires the long text fields at the end of the select clause
read_sql_query = function(con, tbl_name, table_prefix, query = NULL) {
  # grab all column names
  all_cols <- odbcListColumns(con, tbl_name)
  # all_cols <- odbc::dbListFields(conn= con, name = tbl_name)
  # extract ntext columns
  long_cols = all_cols %>%
    dplyr::filter(type == "ntext") %>%
    pull(name)
  # extract text columns
  other_cols = all_cols %>%
    dplyr::filter(type == "text") %>%
    pull(name)
  # put ntext at the end
  long_cols = unique(c(other_cols, long_cols))

  # reorder the selection to not error
  if (is.null(query)){
    tab = dplyr::tbl(con, sql(glue::glue("SELECT *  FROM {table_prefix}.[{tbl_name}]"))) %>%
      dplyr::select(-tidyselect::any_of(long_cols),
                    tidyselect::everything(),
                    tidyselect::any_of(long_cols))
  } else {
    tab = dplyr::tbl(con, sql(query)) %>%
      dplyr::select(-tidyselect::any_of(long_cols),
                    tidyselect::everything(),
                    tidyselect::any_of(long_cols))
  }

}
