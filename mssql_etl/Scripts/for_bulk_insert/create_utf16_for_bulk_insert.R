# Sample data frame
# df <- data.frame(
#   Series_reference = "W_A11",
#   Period = "2002-04",
#   Type = "Moving average",
#   Data_value = 59,
#   Lower_CI = 50.308125050352,
#   Upper_CI = 67.6918749496479,
#   Units = "Injuries",
#   Indicator = "Number",
#   Cause = "Assault",
#   Validation = "Validated",
#   Population = "Whole pop",
#   Age = "All ages",
#   Severity = "Fatal",
#   stringsAsFactors = FALSE
# )
#
# # Define the file path
# file_path <- "test_data.csv"
#
# # Open a connection to the file with UTF-16 encoding
# con <- file(file_path, open = "w", encoding = "UTF-16LE")
#
# # Write the data to the file
# write.csv(df, con, row.names = FALSE)
#
# # Close the connection
# close(con)

convert_to_utf16 <- function(input_file_path) {
  # Load necessary libraries
  library(readr)

  # Read the CSV file
  data <- read_csv(input_file_path)

  # Convert character columns to UTF-16
  df_utf16 <- data
  char_cols <- sapply(df_utf16, is.character)
  df_utf16[char_cols] <- lapply(df_utf16[char_cols], iconv, to = "UTF-16LE")

  # Create the new file path by appending '_UTF16' before the file extension
  file_parts <- strsplit(input_file_path, split = "\\.")[[1]]
  output_file_path <- paste0(
    paste(file_parts[-length(file_parts)], collapse = "."),
    "_UTF16.",
    file_parts[length(file_parts)]
  )

  # Write to CSV with UTF-16 encoding
  write.csv(
    df_utf16,
    output_file_path,
    row.names = FALSE,
    fileEncoding = "UTF-16LE"
  )
  # print a message to the console
  message("File converted to UTF-16: ", output_file_path)

  # Return the path of the new file
  return(output_file_path)
}


# Define the file path
file_path <- "\\\\DECIMAL\\HealthFilesAnalytics\\test_data\\BC Stat Population Estimates_20250227.csv"
# convert it to UTF-16
convert_to_utf16(file_path)
