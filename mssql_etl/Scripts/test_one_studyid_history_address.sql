-- =============================================
-- Step 0: Initialize Timing Variables
-- =============================================

DECLARE @StartTime DATETIME2, @EndTime DATETIME2, @DurationSeconds FLOAT;

-- =============================================
-- Step 1: Clean and Prepare Data (Using Staging Table)
-- =============================================

SET @StartTime = SYSDATETIME();

-- Drop temporary tables if they already exist
IF OBJECT_ID('tempdb..#CleanedData') IS NOT NULL DROP TABLE #CleanedData;

-- Select and transform data from the staging table
SELECT
    STUDY_ID,
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
FROM [HealthFiles_test].[dev].[VIEW_COMBINED_HEALTH_CLIENT]
WHERE STUDY_ID = '001A9042FA9A48CC00E28DD031448F25B9CE08AA676282D6'; -- Replace with your STUDY_ID

-- Create indexes to optimize subsequent operations
CREATE NONCLUSTERED INDEX IX_CleanedData_EFF_DATE ON #CleanedData (STUDY_ID, EFF_DATE);

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 1: Clean and Prepare Data took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 2: Compute Previous Values Using Window Functions
-- =============================================

SET @StartTime = SYSDATETIME();

-- Create and populate #LaggedData with previous postal code and street words
IF OBJECT_ID('tempdb..#LaggedData') IS NOT NULL DROP TABLE #LaggedData;

SELECT
    CD.STUDY_ID,
    CD.POSTAL_CODE,
    CD.STREET_LINE,
    CD.STREET_FIRST_TWO_WORDS,
    CD.EFF_DATE,
    CD.END_DATE,
    LAG(CD.POSTAL_CODE) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE) AS Prev_POSTAL_CODE,
    LAG(CD.STREET_FIRST_TWO_WORDS) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE) AS Prev_STREET_FIRST_TWO_WORDS
INTO #LaggedData
FROM #CleanedData CD
ORDER BY CD.EFF_DATE;

-- Create index to optimize window functions
CREATE NONCLUSTERED INDEX IX_LaggedData_EFF_DATE ON #LaggedData (EFF_DATE);

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 2: Compute Previous Values Using Window Functions took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 3: Flag Changes in Address
-- =============================================

SET @StartTime = SYSDATETIME();

-- Create and populate #ChangeFlagData with ChangeFlag
IF OBJECT_ID('tempdb..#ChangeFlagData') IS NOT NULL DROP TABLE #ChangeFlagData;

SELECT
    LD.STUDY_ID,
    LD.POSTAL_CODE,
    LD.STREET_LINE,
    LD.STREET_FIRST_TWO_WORDS,
    LD.EFF_DATE,
    LD.END_DATE,
    LD.Prev_POSTAL_CODE,
    LD.Prev_STREET_FIRST_TWO_WORDS,
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
ORDER BY LD.EFF_DATE;

-- Create index to optimize subsequent operations
CREATE NONCLUSTERED INDEX IX_ChangeFlagData_EFF_DATE ON #ChangeFlagData (EFF_DATE);

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 3: Flag Changes in Address took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 4: Assign GroupAddressKey Using Cumulative Sum
-- =============================================

SET @StartTime = SYSDATETIME();

-- Create and populate #GroupedKeyData with GroupAddressKey
IF OBJECT_ID('tempdb..#GroupedKeyData') IS NOT NULL DROP TABLE #GroupedKeyData;

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
    SUM(CASE WHEN CFD.ChangeFlag = 1 THEN 1 ELSE 0 END) OVER (ORDER BY CFD.EFF_DATE ROWS UNBOUNDED PRECEDING) AS GroupAddressKey
INTO #GroupedKeyData
FROM #ChangeFlagData CFD
ORDER BY CFD.EFF_DATE;

-- Create index to optimize aggregation
CREATE NONCLUSTERED INDEX IX_GroupedKeyData_GroupAddressKey ON #GroupedKeyData (GroupAddressKey);

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 4: Assign GroupAddressKey Using Cumulative Sum took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 5: Aggregate Grouped Data
-- =============================================

SET @StartTime = SYSDATETIME();

-- Create and populate #GroupedData with aggregated information
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
GROUP BY GK.STUDY_ID, GK.POSTAL_CODE, GK.STREET_FIRST_TWO_WORDS, GK.GroupAddressKey;

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 5: Aggregate Grouped Data took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 6: Insert Aggregated Data into Target Table (Optimized Join)
-- =============================================

SET @StartTime = SYSDATETIME();
-- Step 6.1: Delete existing records for STUDY_IDs present in #GroupedData
DELETE D
FROM DEV.FCT_HEALTH_CLIENT_ADDRESS_DATE D
INNER JOIN #GroupedData GD
    ON D.STUDY_ID = GD.STUDY_ID;
PRINT 'Existing records for STUDY_IDs have been deleted from DEV.FCT_HEALTH_CLIENT_ADDRESS_DATE.';
    
-- Insert the processed data into the target table
INSERT INTO DEV.FCT_HEALTH_CLIENT_ADDRESS_DATE (
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

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 6: Insert Aggregated Data into Target Table took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 7: Cleanup Temporary Tables
-- =============================================

SET @StartTime = SYSDATETIME();

DROP TABLE IF EXISTS #CleanedData, #LaggedData, #ChangeFlagData, #GroupedKeyData, #GroupedData;

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 7: Cleanup Temporary Tables took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';
