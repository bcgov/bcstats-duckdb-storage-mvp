-- =============================================
-- Step 0: Initialize Timing Variables
-- =============================================

DECLARE @StartTime1 DATETIME2;
DECLARE @StartTime DATETIME2, @EndTime DATETIME2, @DurationSeconds FLOAT;
DECLARE @BatchSize INT = 1; -- Increased batch size for efficiency
DECLARE @ProcessedCount INT = 0;
DECLARE @TotalCount INT;

-- Create a logging table if it doesn't exist
IF OBJECT_ID('tempdb..#BatchLog') IS NOT NULL DROP TABLE #BatchLog;

CREATE TABLE #BatchLog (
    BatchNumber INT IDENTITY(1,1) PRIMARY KEY,
    STUDY_ID NVARCHAR(50),
    StartTime DATETIME2,
    EndTime DATETIME2,
    DurationSeconds FLOAT,
    Status NVARCHAR(50)
);

-- =============================================
-- Step 1: Initialize the StudyID List
-- =============================================

-- Create a temporary table to hold STUDY_IDs to process
IF OBJECT_ID('tempdb..#StudyIDList') IS NOT NULL DROP TABLE #StudyIDList;

SELECT STUDY_ID
INTO #StudyIDList
FROM dev.DIM_HEALTH_CLIENT_ID_TEST;

-- Get the total number of STUDY_IDs to process
SELECT @TotalCount = COUNT(*) FROM #StudyIDList;

PRINT 'Total STUDY_IDs to process: ' + CAST(@TotalCount AS NVARCHAR);

-- =============================================
-- Step 2: Batch Processing Loop
-- =============================================

WHILE EXISTS (SELECT 1 FROM #StudyIDList)
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        -- Initialize Timing for the Batch
        SET @StartTime1 = SYSDATETIME();

        IF OBJECT_ID('tempdb..#CurrentBatch') IS NOT NULL DROP TABLE #CurrentBatch;
        -- Select the top @BatchSize STUDY_IDs for this batch
        SELECT TOP (@BatchSize) STUDY_ID
        INTO #CurrentBatch
        FROM #StudyIDList
        ORDER BY STUDY_ID; -- Optional: Define order for consistency

        -- =============================================
        -- Step 3: Clean and Prepare Data (Using Staging Table)
        -- =============================================

        -- Drop temporary tables if they already exist
        IF OBJECT_ID('tempdb..#CleanedData') IS NOT NULL DROP TABLE #CleanedData;

        -- Select and transform data from the staging table for the current batch
        SELECT
            c.STUDY_ID,
            POSTAL_CODE,
            STREET_LINE,
            -- Extract the first two words from STREET_LINE
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
            EFF_DATE,
            END_DATE
        INTO #CleanedData
        FROM [HealthFiles_test].[dev].[VIEW_COMBINED_HEALTH_CLIENT] S
        INNER JOIN #CurrentBatch C
            ON S.STUDY_ID = C.STUDY_ID;

        -- Create indexes to optimize subsequent operations
        CREATE NONCLUSTERED INDEX IX_CleanedData_EFF_DATE ON #CleanedData (STUDY_ID, EFF_DATE);
        
        SET @EndTime = SYSDATETIME();
        SET @DurationSeconds = DATEDIFF(SECOND, @StartTime1, @EndTime);
        PRINT 'Step 3: Clean and Prepare Data took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

        -- =============================================
        -- Step 4: Compute Previous Values Using Window Functions
        -- =============================================
        SET @StartTime = SYSDATETIME();
        -- Drop temporary tables if they already exist
        IF OBJECT_ID('tempdb..#LaggedData') IS NOT NULL DROP TABLE #LaggedData;

        -- Create and populate #LaggedData with previous postal code and street words
        SELECT
            CD.STUDY_ID,
            CD.POSTAL_CODE,
            CD.STREET_LINE,
            CD.STREET_FIRST_TWO_WORDS,
            CD.EFF_DATE,
            CD.END_DATE,
            CD.RecordID,
            LAG(CD.POSTAL_CODE) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE, CD.RecordID) AS Prev_POSTAL_CODE,
            LAG(CD.STREET_FIRST_TWO_WORDS) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE, CD.RecordID) AS Prev_STREET_FIRST_TWO_WORDS
        INTO #LaggedData
        FROM (
            SELECT 
                *, 
                ROW_NUMBER() OVER (PARTITION BY STUDY_ID ORDER BY EFF_DATE) AS RecordID
            FROM #CleanedData
        ) CD
        ORDER BY CD.EFF_DATE, CD.RecordID;

        -- Create index to optimize window functions
        -- CREATE NONCLUSTERED INDEX IX_LaggedData_EFF_DATE ON #LaggedData (EFF_DATE);

        SET @EndTime = SYSDATETIME();
        SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Step 4: Compute Previous Values Using Window Functions took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

        -- =============================================
        -- Step 5: Flag Changes in Address
        -- =============================================

        SET @StartTime = SYSDATETIME();

        -- Drop temporary tables if they already exist
        IF OBJECT_ID('tempdb..#ChangeFlagData') IS NOT NULL DROP TABLE #ChangeFlagData;

        -- Create and populate #ChangeFlagData with ChangeFlag
        SELECT
            LD.STUDY_ID,
            LD.POSTAL_CODE,
            LD.STREET_LINE,
            LD.STREET_FIRST_TWO_WORDS,
            LD.EFF_DATE,
            LD.END_DATE,
            LD.Prev_POSTAL_CODE,
            LD.Prev_STREET_FIRST_TWO_WORDS,
            LD.RecordID,
            CASE 
                WHEN LD.Prev_POSTAL_CODE IS NULL 
                     OR LD.Prev_STREET_FIRST_TWO_WORDS IS NULL 
                     OR LD.Prev_POSTAL_CODE != LD.POSTAL_CODE 
                     OR LD.Prev_STREET_FIRST_TWO_WORDS != LD.STREET_FIRST_TWO_WORDS 
                THEN 1 
                ELSE 0 
            END AS ChangeFlag
        INTO #ChangeFlagData
        FROM #LaggedData LD
        ORDER BY LD.STUDY_ID, LD.EFF_DATE, LD.RecordID;

        -- Create index to optimize subsequent operations
        -- CREATE NONCLUSTERED INDEX IX_ChangeFlagData_EFF_DATE ON #ChangeFlagData (EFF_DATE);

        SET @EndTime = SYSDATETIME();
        SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Step 5: Flag Changes in Address took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';


        -- =============================================
        -- Step 6: Assign GroupAddressKey Using Cumulative Sum
        -- =============================================
        
        SET @StartTime = SYSDATETIME();

        -- Drop temporary tables if they already exist
        IF OBJECT_ID('tempdb..#GroupedKeyData') IS NOT NULL DROP TABLE #GroupedKeyData;

        -- Create and populate #GroupedKeyData with GroupAddressKey
        SELECT
            CFD.STUDY_ID,
            CFD.POSTAL_CODE,
            CFD.STREET_LINE,
            CFD.STREET_FIRST_TWO_WORDS,
            CFD.EFF_DATE,
            CFD.END_DATE,
            CFD.Prev_POSTAL_CODE,
            CFD.Prev_STREET_FIRST_TWO_WORDS,
            CFD.ChangeFlag,
            -- Cumulative sum to assign GroupAddressKey
            SUM(CASE WHEN CFD.ChangeFlag = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY CFD.STUDY_ID ORDER BY CFD.EFF_DATE, CFD.RecordID ROWS UNBOUNDED PRECEDING) AS GroupAddressKey
        INTO #GroupedKeyData
        FROM #ChangeFlagData CFD
        ORDER BY CFD.STUDY_ID, CFD.EFF_DATE, CFD.RecordID;

        SET @EndTime = SYSDATETIME();
        SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Step 6: Assign GroupAddressKey Using Cumulative Sum took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

        -- =============================================
        -- Step 7: Aggregate Grouped Data
        -- =============================================

        SET @StartTime = SYSDATETIME();

        -- Drop temporary tables if they already exist
        IF OBJECT_ID('tempdb..#GroupedData') IS NOT NULL DROP TABLE #GroupedData;

        -- Create and populate #GroupedData with aggregated information
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
        GROUP BY GK.STUDY_ID, GK.POSTAL_CODE, GK.STREET_FIRST_TWO_WORDS, GK.GroupAddressKey;

        SET @EndTime = SYSDATETIME();
        SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Step 7: Aggregate Grouped Data took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

        -- =============================================
        -- Step 8: Insert Aggregated Data into Target Table (Optimized Join)
        -- =============================================

        SET @StartTime = SYSDATETIME();

        -- Step 8.1: Delete existing records for STUDY_IDs present in #GroupedData
        DELETE D
        FROM DEV.FCT_HEALTH_CLIENT_ADDRESS_HISTORY D
        INNER JOIN #GroupedData GD
            ON D.STUDY_ID = GD.STUDY_ID;
        PRINT 'Existing records for STUDY_IDs have been deleted from DEV.FCT_HEALTH_CLIENT_ADDRESS_HISTORY.';

        -- Step 8.2: Insert the processed data into the target table
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
            AND GD.STREET_LINE = B.STREET_LINE;
        PRINT 'New records have been inserted into DEV.FCT_HEALTH_CLIENT_ADDRESS_HISTORY.';

        -- Step 8.3: Remove processed STUDY_IDs from #StudyIDList
        DELETE FROM #StudyIDList
        WHERE STUDY_ID IN (SELECT STUDY_ID FROM #GroupedData);
        PRINT 'Processed STUDY_IDs have been removed from #StudyIDList.';

        SET @EndTime = SYSDATETIME();
        SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Step 8: Insert Aggregated Data into Target Table took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

        -- =============================================
        -- Step 9: Cleanup Temporary Tables for the Batch
        -- =============================================

        -- Calculate Duration for the Batch
        SET @EndTime = SYSDATETIME();
        SET @DurationSeconds = DATEDIFF(SECOND, @StartTime1, @EndTime);
        SET @ProcessedCount = @ProcessedCount + (SELECT COUNT(*) FROM #CurrentBatch);
        
        PRINT 'Processed ' + CAST(@ProcessedCount AS NVARCHAR) + ' out of ' + CAST(@TotalCount AS NVARCHAR) + ' STUDY_IDs. Current Batch Duration: ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

        -- Inside the loop, after each batch
        INSERT INTO #BatchLog (STUDY_ID, StartTime, EndTime, DurationSeconds, Status)
        SELECT GD.STUDY_ID, @StartTime1, @EndTime, @DurationSeconds, 'Processed' 
        FROM #GroupedData GD;


        
        COMMIT TRANSACTION;

        IF  @ProcessedCount >= @TotalCount
        BREAK;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT 'An error occurred during batch processing.';
        PRINT ERROR_MESSAGE();

        -- Retrieve error information
        -- DECLARE @ErrorMessage NVARCHAR(4000);
        -- DECLARE @ErrorSeverity INT;
        -- DECLARE @ErrorState INT;
        
        -- SELECT 
        --     @ErrorMessage = ERROR_MESSAGE(),
        --     @ErrorSeverity = ERROR_SEVERITY(),
        --     @ErrorState = ERROR_STATE();
        
        -- Raise the error to notify the calling environment
        -- RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

        -- Optionally, remove the problematic STUDY_IDs to prevent infinite looping
        -- DELETE FROM #StudyIDList
        -- WHERE STUDY_ID IN (SELECT STUDY_ID FROM #CurrentBatch);
        
        -- Decide whether to continue or exit
        -- For example, exit the loop to prevent further issues
        BREAK;
    END CATCH

    -- Drop temporary tables
    DROP TABLE IF EXISTS #CleanedData, #LaggedData, #ChangeFlagData, #GroupedKeyData, #GroupedData, #CurrentBatch;

END

PRINT 'All batches processed successfully.';
