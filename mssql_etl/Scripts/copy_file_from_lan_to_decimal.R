# identify the folders

# This path is retrieved from the configuration file
lan_csv_file_path = config::get("lan_csv_file_path")

current.folder <- file.path(
  lan_csv_file_path,
  "DATABASE\\Citrix\\population by CSD"
)
new.folder <- "\\\\DECIMAL\\HealthFilesAnalytics\\test_data"

list.files(current.folder)
list.files(new.folder)

# find the files that you want to copy
list.of.files <- list.files(
  path = current.folder,
  pattern = "BC Stat Population Estimates_",
  full.names = TRUE
)


print(list.of.files)
# copy the files to the new folder
file.copy(list.of.files, new.folder)
