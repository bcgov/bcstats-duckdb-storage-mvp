# create meta data for CSV files, get the name, path, etc. So project managers could manually choose which one needs to be loaded to SQL Server.

# Load required library
library(tidyverse)
library(fs)
library(stringr)
# Specify the root folder path

root_folder_list <- c(
  file.path(lan_csv_file_path,
            "Population Estimates/Sub-Provincial (Annual)/01_Health Monthly Client Data/"),
  file.path(lan_csv_file_path,
            "Population Estimates/Sub-Provincial (Annual)/01_Health Monthly Client Data/")
)


root_folder <- file.path(lan_csv_file_path,
                         "Population Estimates/Sub-Provincial (Annual)/01_Health Monthly Client Data/")

# Function to convert to valid SQL Server table names
convert_to_sql_name <- function(name) {
  name %>%
    str_replace_all("\\-", "_") %>%     # Replace - with underscores
    str_replace_all("[[:space:]]+", "_") %>%  # Matches all types of spaces
    str_remove_all("[^a-zA-Z0-9_]") %>%      # Removes non-alphanumeric characters
    str_to_upper()                           # Converts to lowercase
}

# Function to get human-readable file sizes
get_human_readable_size <- function(file_path) {
  size_bytes <- file_size(file_path)  # Get file size in bytes
  format(size_bytes, units = "auto")  # Convert to KB, MB, GB, etc.
}

# Function to get CSV files and their subfolder paths
list_csv_files <- function(folder_path) {
  # Normalize the root folder path using fs
  normalized_root <- path_abs(folder_path)

  # List all files recursively using fs
  files <- dir_ls(folder_path, recurse = TRUE, type = "file")

  # Filter for CSV files
  csv_files <- files[grepl("\\.csv$", files, ignore.case = TRUE)]

  # Create a data frame with additional columns
  data <- tibble(
    file_name = path_file(csv_files),  # Extract just the file name
    file_name_no_suffix = str_remove(path_file(csv_files), "\\.csv$"),
    sql_table_name = map_chr(str_remove(path_file(csv_files), "\\.csv$"), convert_to_sql_name),
    relative_path = path_rel(csv_files, start = normalized_root),  # Relative path from root folder
    subfolder_path = path_dir(csv_files),  # Full directory path
    file_size = map_chr(csv_files, get_human_readable_size)  # Get file size in human-readable format
  )

  return(data)
}

# Call the function
csv_files_info <- list_csv_files(root_folder)

# Display the result
print(csv_files_info)


# Optionally, save the results to a CSV
write_csv(csv_files_info, file.path(root_folder,"csv_files_list_with_sql_names.csv"))

csv_files_info %>%
  filter(str_detect(file_name, pattern = "CLR_EXT|BC Stat Population Estimates")) %>%
  filter(!str_detect(file_name, pattern = "bad")) %>%
  write_csv( file.path(root_folder,"clr_ext_csv_files_list_with_sql_names.csv"))

