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

library(stringr)

# Clean and standardize the STREET_LINE column
clean_street_line <- function(street_line, city) {
  street_line %>%
    str_to_upper() %>%                    # Convert to uppercase
    str_squish() %>%                      # Remove extra whitespace
    str_replace_all("[^A-Z0-9 ]", "") %>% # Remove non-alphanumeric characters
    str_replace_all("\\bBC\\b", "") %>%       # Remove BC abbreviation if redundant
    str_replace_all(glue::glue("\\b{city}\\b"), "") %>%
    str_squish()
    }

library(dplyr)

# Sample data
data <- data.frame(
  STUDY_ID = c("01", "01"),
  BIRTH_YR_MON = c("199802", "199802"),
  SEX = c("M", "M"),
  POSTAL_CODE = c("V9C4L1", "V9C4L1"),
  CITY = c("VICTORIA", "VICTORIA"),
  STREET_LINE = c("3574 PROMENADE CRES", "3574 PROMENADE CRES BC VICTORIA"),
  EFF_DATE = as.Date(c("2017-06-01", "2017-01-01")),
  END_DATE = as.Date(c("2019-10-31", "2017-01-31"))
)

# Clean the STREET_LINE column
data <- data %>%
  mutate(STREET_LINE_CLEAN = clean_street_line(STREET_LINE, CITY))

# Group by and summarize
result <- data %>%
  group_by(STUDY_ID, BIRTH_YR_MON, SEX, POSTAL_CODE, CITY, STREET_LINE_CLEAN) %>%
  summarize(
    EFF_DATE = min(EFF_DATE, na.rm = TRUE),
    END_DATE = max(END_DATE, na.rm = TRUE),
    .groups = "drop"
  )

print(result)


result <- data %>%
  group_by(STUDY_ID, BIRTH_YR_MON, SEX, POSTAL_CODE, CITY) %>%
  summarize(
    STREET_LINE = STREET_LINE[which.max(nchar(STREET_LINE))], # Choose longest string
    EFF_DATE = min(EFF_DATE, na.rm = TRUE),
    END_DATE = max(END_DATE, na.rm = TRUE),
    .groups = "drop"
  )

