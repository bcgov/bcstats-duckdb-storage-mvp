
WITH 
CleanedData AS (
    SELECT
        STUDY_ID,
        POSTAL_CODE,
        STREET_LINE,
        CASE 
            WHEN STREET_LINE IS NOT NULL  THEN
                CASE
                    -- Ensure there are at least two words in the cleaned street line
                    WHEN CHARINDEX(' ', STREET_LINE) > 0 THEN
                        LEFT(
                            STREET_LINE,
                            CHARINDEX(' ', STREET_LINE + ' ', CHARINDEX(' ', STREET_LINE + ' ') + 1) - 1
                        )
                    ELSE STREET_LINE -- If no space found, use the entire cleaned street line
                END
            ELSE STREET_LINE
        END AS STREET_FIRST_TWO_WORDS,
        CAST(EFF_DATE  AS DATE) AS EFF_DATE,
        CAST(END_DATE  AS DATE) AS END_DATE
    FROM [HealthFiles_test].[dev].[FCT_COMBINED_HEALTH_CLIENT_TABLE]
),

LaggedAddressData AS (
    SELECT
        *,
        -- Get the previous postal code and street for each record
        LAG(POSTAL_CODE) OVER (PARTITION BY STUDY_ID ORDER BY EFF_DATE) AS Prev_POSTAL_CODE,
        LAG(STREET_FIRST_TWO_WORDS) OVER (PARTITION BY STUDY_ID ORDER BY EFF_DATE) AS Prev_STREET_FIRST_TWO_WORDS
    FROM CleanedData
),
ChangeFlagData AS (
    SELECT
        *,
        -- Create a change flag based on differences in postal code or street
        CASE 
            WHEN Prev_POSTAL_CODE IS NULL OR Prev_STREET_FIRST_TWO_WORDS IS NULL 
                 OR Prev_POSTAL_CODE != POSTAL_CODE 
                 OR Prev_STREET_FIRST_TWO_WORDS != STREET_FIRST_TWO_WORDS 
            THEN 1 
            ELSE 0 
        END AS ChangeFlag
    FROM LaggedAddressData
),

GroupedKeyData AS (
    SELECT
        *, 
        -- Generate the GroupAddressKey by accumulating the change flags
        SUM(ChangeFlag) OVER (PARTITION BY STUDY_ID ORDER BY EFF_DATE, ChangeFlag) AS GroupAddressKey
    FROM ChangeFlagData
), 

groupeddata as (
SELECT
    STUDY_ID,
    POSTAL_CODE,
    STREET_FIRST_TWO_WORDS,
    MAX(STREET_LINE) AS STREET_LINE,
    MIN(EFF_DATE) AS EFF_DATE,
    MAX(END_DATE) AS END_DATE,
    GroupAddressKey
FROM GroupedKeyData
GROUP BY STUDY_ID, POSTAL_CODE, STREET_FIRST_TWO_WORDS, GroupAddressKey
)
SELECT     A.STUDY_ID,
       A.GroupAddressKey,
       A.POSTAL_CODE,
       A.STREET_LINE,
       A.EFF_DATE,
       A.END_DATE,       
       B.[CITY], 
       B.LATITUDE, 
       B.LONGITUDE
FROM  groupeddata A
LEFT JOIN dev.DIM_HEALTH_CLIENT_ADDRESS B
ON A.POSTAL_CODE = B.POSTAL_CODE AND A.STREET_LINE = B.STREET_LINE 
WHERE STUDY_ID = '001A9042FA9A48CC00E28DD031448F25B9CE08AA676282D6'
ORDER BY STUDY_ID, GroupAddressKey;






