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
source(file = "./r-implementation/utils/utils.r")

# Define the path to the test CSV folder
# This path is retrieved from the configuration file
test_csv_folder = config::get("test_sql_server_csv")

# Define the folder path
folder_path <- file.path(test_csv_folder, "raw_data")

# List all files in the folder with their full paths
file_list <- list.files(path = folder_path, full.names = TRUE)

# Get file information, including size
file_info <- file.info(file_list)

# Extract the file name and size, and put them in a data frame
file_table <- data.frame(
  file_name = basename(file_list),  # Get only the file name
  # file_size = file_info$size        # File size in bytes
  file_size_MB = round(file_info$size / (1024^2), 2)  # File size in MB, rounded to 2 decimal places
)

# Print the table
print(file_table)


# Convert the file_table to Markdown format
markdown_table <- paste0("| ", paste(names(file_table), collapse = " | "), " |\n",
                         "| ", paste(rep("---", ncol(file_table)), collapse = " | "), " |\n",
                         paste(apply(file_table, 1, function(row) paste("|", paste(row, collapse = " | "), "|")), collapse = "\n"))

# Print the Markdown table
cat(markdown_table)
