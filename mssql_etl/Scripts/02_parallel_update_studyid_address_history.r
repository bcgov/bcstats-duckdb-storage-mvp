# Load required libraries
library(DBI)
library(odbc)
library(dplyr)
library(lubridate)
library(doParallel)
library(foreach)

# -----------------------------
# Database Connection Parameters
# -----------------------------

# ---- Configuration ----
# prod database
db_config <- config::get("decimal")
my_schema <- config::get("myschema")
# ---- Connection to decimal ----
# decimal_conn <- dbConnect(odbc::odbc(),
#                           Driver = db_config$driver,
#                           Server = db_config$server,
#                           Database = db_config$database,
#                           Trusted_Connection = "True")

# Function to open a new connection (to be used in workers)
open_db_connection <- function() {
  con <- dbConnect(odbc::odbc(),
                   Driver = db_config$driver,
                   Server = db_config$server,
                   Database = db_config$database,
                   Trusted_Connection = "True")
  return(con)
}

# -----------------------------
# Helper Function: Update Status (for a given connection)
# -----------------------------
update_status <- function(con, study_id, new_status) {
  update_query <- sprintf("UPDATE dev.DIM_HEALTH_CLIENT_ID
                           SET Status = '%s'
                           WHERE STUDY_ID IN ('%s');", new_status, study_id)
  dbExecute(con, update_query)
  cat(sprintf("Updated STUDY_ID %s status to '%s'.\n", study_id, new_status))
}

# -----------------------------
# Worker Function: Process a Single STUDY_ID
# -----------------------------
process_study_id_worker <- function(study_id) {
  # Each worker opens its own connection
  con_worker <- open_db_connection()
  on.exit(dbDisconnect(con_worker), add = TRUE)

  # Mark the study_id as 'Processing' (this should already be set by the master,
  # but doing it here ensures consistency in case of race conditions)
  update_status(con_worker, study_id, "Processing")
  start_time <- Sys.time()

  # Build the SQL script for processing
  sql_script <- sprintf("

    -- =============================================
    -- Step 1: Clean and Prepare Data (Using Staging Table)
    -- =============================================
    -- Drop temporary tables if they already exist
    IF OBJECT_ID('tempdb..#CleanedData') IS NOT NULL DROP TABLE #CleanedData;

    -- Select and transform data from the staging table
    SELECT
        S.STUDY_ID,
        POSTAL_CODE,
        STREET_LINE,
        ESTIMATED_EFF_DATE as EFF_DATE,
        ESTIMATED_END_DATE AS END_DATE,
        CASE
        WHEN STREET_LINE IS NOT NULL THEN
            CASE
                WHEN CHARINDEX(' ', STREET_LINE) > 0 THEN
                    LEFT(
                        STREET_LINE,
                        CHARINDEX(' ', STREET_LINE + ' ', CHARINDEX(' ', STREET_LINE + ' ') + 1) - 1
                    )
                ELSE STREET_LINE
            END
        ELSE STREET_LINE
        END AS STREET_FIRST_TWO_WORDS,
        ROW_NUMBER() OVER (PARTITION BY STUDY_ID ORDER BY ESTIMATED_EFF_DATE, ESTIMATED_END_DATE) AS RecordID
        INTO #CleanedData
    FROM [HealthFiles_test].[dev].[VIEW_COMBINED_HEALTH_CLIENT] S
    WHERE s.STUDY_ID IN ('%s')
    -- Create indexes to optimize subsequent operations
    CREATE NONCLUSTERED INDEX IX_CleanedData_EFF_DATE ON #CleanedData (STUDY_ID, RecordID);

    -- =============================================
    -- Step 2: Compute Previous Values Using Window Functions
    -- =============================================
    IF OBJECT_ID('tempdb..#LaggedData') IS NOT NULL DROP TABLE #LaggedData;
    SELECT
    CD.STUDY_ID,
    CD.POSTAL_CODE,
    CD.STREET_LINE,
    CD.STREET_FIRST_TWO_WORDS,
    CD.EFF_DATE,
    CD.END_DATE,
    CD.RecordID,
    LAG(CD.POSTAL_CODE) OVER (PARTITION BY CD.STUDY_ID ORDER BY  CD.RecordID) AS Prev_POSTAL_CODE,
    LAG(CD.STREET_LINE) OVER (PARTITION BY CD.STUDY_ID ORDER BY  CD.RecordID) AS Prev_STREET_LINE,
    LAG(CD.STREET_FIRST_TWO_WORDS ) OVER (PARTITION BY CD.STUDY_ID ORDER BY  CD.RecordID) AS Prev_STREET_FIRST_TWO_WORDS
    INTO #LaggedData
    FROM  #CleanedData CD
    ORDER BY CD.STUDY_ID, CD.RecordID;
    CREATE NONCLUSTERED INDEX IX_LaggedData_STUDY_ID_RecordID ON #LaggedData (STUDY_ID, RecordID);

    -- =============================================
    -- Step 3: Flag Changes in Address
    -- =============================================
    IF OBJECT_ID('tempdb..#ChangeFlagData') IS NOT NULL DROP TABLE #ChangeFlagData;
    SELECT
        LD.STUDY_ID,
        LD.POSTAL_CODE,
        LD.STREET_LINE,
        LD.STREET_FIRST_TWO_WORDS,
        LD.EFF_DATE,
        LD.END_DATE,
        LD.RecordID,
        CASE
            WHEN LD.Prev_POSTAL_CODE IS NULL THEN 1
            WHEN ISNULL(LD.Prev_POSTAL_CODE, '') != ISNULL(LD.POSTAL_CODE, '')   THEN 1  -- it is rare that postal code is null
            -- Compare street_line values treating NULL as an empty string.
            -- WHEN ISNULL(LD.Prev_STREET_LINE, '') <> ISNULL(LD.STREET_LINE, '') THEN 1  -- STREET LINE are messy, could have typo etc.
            WHEN ISNULL(LD.Prev_STREET_FIRST_TWO_WORDS, '') <> ISNULL(LD.STREET_FIRST_TWO_WORDS, '') THEN 1  -- STREET_FIRST_TWO_WORDS are much clean
            ELSE 0
        END AS ChangeFlag
     INTO #ChangeFlagData
    FROM #LaggedData LD
    ORDER BY LD.STUDY_ID,LD.RecordID;
    CREATE NONCLUSTERED INDEX IX_ChangeFlagData_STUDY_ID_RecordID ON #ChangeFlagData (STUDY_ID, RecordID);


    -- =============================================
    -- Step 4: Assign GroupAddressKey Using Cumulative Sum
    -- =============================================
    IF OBJECT_ID('tempdb..#GroupedKeyData') IS NOT NULL DROP TABLE #GroupedKeyData;
    SELECT
        CFD.STUDY_ID,
        CFD.POSTAL_CODE,
        CFD.STREET_LINE,
        CFD.STREET_FIRST_TWO_WORDS,
        CFD.EFF_DATE,
        CFD.END_DATE,
        CFD.RecordID,
        CFD.ChangeFlag,
        -- Cumulative sum to assign GroupAddressKey
        SUM(CASE WHEN CFD.ChangeFlag = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY CFD.STUDY_ID ORDER BY CFD.RecordID ROWS UNBOUNDED PRECEDING) AS GroupAddressKey
    INTO #GroupedKeyData
    FROM #ChangeFlagData CFD
    ORDER BY CFD.STUDY_ID, CFD.RecordID;
    CREATE NONCLUSTERED INDEX IX_GroupedKeyData_GroupAddressKey ON #GroupedKeyData (GroupAddressKey);

    -- =============================================
    -- Step 5: Aggregate Grouped Data
    -- =============================================
    IF OBJECT_ID('tempdb..#GroupedData') IS NOT NULL DROP TABLE #GroupedData;
    SELECT
        GK.STUDY_ID,
        GK.POSTAL_CODE,
        GK.STREET_FIRST_TWO_WORDS,
        MAX(GK.STREET_LINE) AS STREET_LINE,
        MIN(GK.EFF_DATE) AS EFF_DATE,
        MAX(GK.END_DATE) AS END_DATE,
        GK.GroupAddressKey
    INTO #GroupedData
    FROM #GroupedKeyData GK
    GROUP BY GK.STUDY_ID, GK.GroupAddressKey, GK.POSTAL_CODE, GK.STREET_FIRST_TWO_WORDS;

    -- =============================================
    -- Step 6: Insert Aggregated Data into Target Table (Optimized Join)
    -- =============================================
    DELETE D
    FROM DEV.FCT_HEALTH_CLIENT_ADDRESS_HISTORY D
    INNER JOIN #GroupedData GD
        ON D.STUDY_ID = GD.STUDY_ID;

    INSERT INTO DEV.FCT_HEALTH_CLIENT_ADDRESS_HISTORY (
        STUDY_ID,
        GroupAddressKey,
        POSTAL_CODE,
        STREET_LINE,
        EFF_DATE,
        END_DATE,
        CITY,
        LATITUDE,
        LONGITUDE
    )
    SELECT
        GD.STUDY_ID,
        GD.GroupAddressKey,
        GD.POSTAL_CODE,
        GD.STREET_LINE,
        GD.EFF_DATE,
        GD.END_DATE,
        B.[CITY],
        B.LATITUDE,
        B.LONGITUDE
    FROM #GroupedData GD
    LEFT JOIN dev.DIM_HEALTH_CLIENT_ADDRESS B
        ON GD.POSTAL_CODE = B.POSTAL_CODE
        AND GD.STREET_LINE = B.STREET_LINE
        ORDER BY GD.STUDY_ID,
        GD.GroupAddressKey;

    -- =============================================
    -- Step 7: Cleanup Temporary Tables
    -- =============================================
    DROP TABLE IF EXISTS #CleanedData, #LaggedData, #ChangeFlagData, #GroupedKeyData, #GroupedData;",
                         study_id)

  # Execute the SQL processing script
  tryCatch({
    dbExecute(con_worker, sql_script)
    update_status(con_worker, study_id, "Processed")
    end_time <- Sys.time()
    duration <- as.numeric(difftime(end_time, start_time, units = "secs"))
    log_entry <- tibble(
      STUDY_ID      = study_id,
      Status        = "Success",
      Start_Time    = start_time,
      End_Time      = end_time,
      Duration_secs = duration,
      Message       = "Processed successfully."
    )
    cat(sprintf("STUDY_ID %s processed successfully in %.2f seconds.\n", study_id, duration))
  }, error = function(e) {
    update_status(con_worker, study_id, "Error")
    end_time <- Sys.time()
    duration <- as.numeric(difftime(end_time, start_time, units = "secs"))
    log_entry <- tibble(
      STUDY_ID      = study_id,
      Status        = "Failed",
      Start_Time    = start_time,
      End_Time      = end_time,
      Duration_secs = duration,
      Message       = e$message
    )
    cat(sprintf("Error processing STUDY_ID %s: %s\n", study_id, e$message))
  })

  return(log_entry)
}

# -----------------------------
# Setup Parallel Backend
# -----------------------------
# Determine number of cores to use (adjust as needed)
num_cores <- parallel::detectCores() - 1
cl <- makeCluster(num_cores)
registerDoParallel(cl)

# -----------------------------
# Main Processing Loop in Batches
# -----------------------------
# We process in batches to avoid loading all pending records.
batch_size <- 100
log_df <- tibble(
  STUDY_ID      = character(),
  Status        = character(),
  Start_Time    = as.POSIXct(character()),
  End_Time      = as.POSIXct(character()),
  Duration_secs = numeric(),
  Message       = character()
)
iteration <- 0

repeat {
  # Fetch a batch of pending STUDY_IDs
  pending_batch <- dbGetQuery(open_db_connection(),
                              sprintf("SELECT TOP %d STUDY_ID FROM dev.DIM_HEALTH_CLIENT_ID WHERE Status = 'Pending'", batch_size))

  if(nrow(pending_batch) == 0) {
    cat("No pending STUDY_ID records found. Exiting processing loop.\n")
    break
  }

  # Mark all pending STUDY_IDs in this batch as 'Processing'
  study_ids_batch <- paste(sprintf("'%s'", pending_batch$STUDY_ID), collapse = ",")
  dbExecute(open_db_connection(),
            sprintf("UPDATE dev.DIM_HEALTH_CLIENT_ID SET Status = 'Processing' WHERE STUDY_ID IN (%s)", study_ids_batch))

  # Process the batch in parallel
  batch_logs <- foreach(study_id = pending_batch$STUDY_ID, .combine = bind_rows,
                        .packages = c("DBI", "odbc", "dplyr", "lubridate")) %dopar% {
                          process_study_id_worker(study_id)
                        }

  # Append the batch logs to the global log
  log_df <- bind_rows(log_df, batch_logs)

  iteration <- iteration + nrow(pending_batch)
  cat(sprintf("Completed processing %d records so far.\n", iteration))

  # Save the log after every 10,000 iterations
  # if (iteration %% 10000 < batch_size) {
  #   write.csv(log_df, "processing_log.csv", row.names = FALSE)
  #   cat(sprintf("Log saved after %d records.\n", iteration))
  # }
}

# Save the final log
write.csv(log_df, "processing_log_final.csv", row.names = FALSE)
cat("Final log saved. Disconnecting from the database and stopping the cluster.\n")

# -----------------------------
# Cleanup: Stop Parallel Cluster and Close Master Connection
# -----------------------------
stopCluster(cl)
# Optionally, if you opened a master connection earlier, disconnect it here.
cat("Database processing complete.\n")
