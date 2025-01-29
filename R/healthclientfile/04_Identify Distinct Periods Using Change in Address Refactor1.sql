-- Step 1: Create a temporary table with cleaned data
SELECT
    STUDY_ID,
    POSTAL_CODE,
    STREET_LINE,
    CITY, -- Include CITY to use in cleaning
    dbo.clean_street_line(STREET_LINE, CITY) AS CleanedStreetLine,
    CAST(EFF_DATE AS DATE) AS EFF_DATE,
    CAST(END_DATE AS DATE) AS END_DATE
INTO #CleanedData
FROM [HealthFiles_test].[dev].[FCT_COMBINED_HEALTH_CLIENT_TABLE]
-- Add an index to optimize subsequent operations
CREATE INDEX IX_CleanedData_Study_EffDate ON #CleanedData (STUDY_ID, EFF_DATE);

-- Step 2: Compute STREET_FIRST_TWO_WORDS and store in the temporary table
ALTER TABLE #CleanedData
ADD STREET_FIRST_TWO_WORDS NVARCHAR(255); -- Adjust size as needed

UPDATE #CleanedData
SET STREET_FIRST_TWO_WORDS = 
    CASE 
        WHEN CleanedStreetLine IS NOT NULL AND CITY IS NOT NULL AND LEN(CleanedStreetLine) > 0 THEN
            CASE
                WHEN CHARINDEX(' ', CleanedStreetLine) > 0 THEN
                    LEFT(
                        CleanedStreetLine,
                        CHARINDEX(' ', CleanedStreetLine + ' ', CHARINDEX(' ', CleanedStreetLine + ' ') + 1) - 1
                    )
                ELSE CleanedStreetLine
            END
        ELSE STREET_LINE
    END;

-- Step 3: Add LAG columns using a temporary table
SELECT
    CD.*,
    LAG(CD.POSTAL_CODE) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE) AS Prev_POSTAL_CODE,
    LAG(CD.STREET_FIRST_TWO_WORDS) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE) AS Prev_STREET_FIRST_TWO_WORDS
INTO #LaggedAddressData
FROM #CleanedData CD;

-- Add an index to optimize window functions
CREATE INDEX IX_LaggedAddressData_Study_EffDate ON #LaggedAddressData (STUDY_ID, EFF_DATE);

-- Step 4: Add ChangeFlag to identify changes in address
SELECT
    LAD.*,
    CASE 
        WHEN Prev_POSTAL_CODE IS NULL 
             OR Prev_STREET_FIRST_TWO_WORDS IS NULL 
             OR Prev_POSTAL_CODE != POSTAL_CODE 
             OR Prev_STREET_FIRST_TWO_WORDS != STREET_FIRST_TWO_WORDS 
        THEN 1 
        ELSE 0 
    END AS ChangeFlag
INTO #ChangeFlagData
FROM #LaggedAddressData LAD;

-- Step 5: Generate GroupAddressKey using cumulative SUM
SELECT
    CFD.*,
    SUM(ChangeFlag) OVER (PARTITION BY CFD.STUDY_ID ORDER BY CFD.EFF_DATE, CFD.ChangeFlag) AS GroupAddressKey
INTO #GroupedKeyData
FROM #ChangeFlagData CFD;

-- Step 6: Aggregate grouped data
SELECT
    STUDY_ID,
    POSTAL_CODE,
    STREET_FIRST_TWO_WORDS,
    MAX(STREET_LINE) AS STREET_LINE,
    MIN(EFF_DATE) AS EFF_DATE,
    MAX(END_DATE) AS END_DATE,
    GroupAddressKey
INTO #GroupedData
FROM #GroupedKeyData
GROUP BY STUDY_ID, POSTAL_CODE, STREET_FIRST_TWO_WORDS, GroupAddressKey;

-- Step 7: Final Insert with JOIN
SELECT 
    A.STUDY_ID,
    A.GroupAddressKey,
    A.POSTAL_CODE,
    A.STREET_LINE,
    A.EFF_DATE,
    A.END_DATE,       
    B.[CITY], 
    B.LATITUDE, 
    B.LONGITUDE
INTO DEV.FCT_HEALTH_CLIENT_ADDRESS_DATE
FROM #GroupedData A
LEFT JOIN dev.DIM_HEALTH_CLIENT_ADDRESS B
    ON A.POSTAL_CODE = B.POSTAL_CODE 
    AND A.STREET_LINE = B.STREET_LINE 
ORDER BY A.STUDY_ID, A.GroupAddressKey;

-- Step 8: Clean up temporary tables
DROP TABLE IF EXISTS #CleanedData, #LaggedAddressData, #ChangeFlagData, #GroupedKeyData, #GroupedData;
