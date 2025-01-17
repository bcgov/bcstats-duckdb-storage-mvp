
WITH CleanedData AS (
    SELECT
        STUDY_ID,
        POSTAL_CODE,
        -- STREET_LINE,
        dbo.clean_street_line(STREET_LINE, CITY) AS STREET_LINE,
         LEFT(STREET_LINE, CHARINDEX(' ', dbo.clean_street_line(STREET_LINE, CITY) + ' ', CHARINDEX(' ', STREET_LINE + ' ') + 1) - 1) AS STREET_FIRST_TWO_WORDS,
        EFF_DATE,
        END_DATE,
        FORMAT(TRY_CONVERT(DATE, EFF_DATE), 'yyyy-MM') AS EFF_MONTH, -- Convert and extract EFF_MONTH
        FORMAT(TRY_CONVERT(DATE, END_DATE), 'yyyy-MM') AS END_MONTH, -- Convert and extract END_MONTH
        -- Calculate lagged END_DATE for comparison
        LAG(END_DATE) OVER (
            PARTITION BY STUDY_ID, POSTAL_CODE, STREET_LINE
            ORDER BY EFF_DATE
        ) AS Prev_END_DATE,
        LAG(FORMAT(TRY_CONVERT(DATE, END_DATE), 'yyyy-MM')) OVER (
            PARTITION BY STUDY_ID, POSTAL_CODE, STREET_LINE
            ORDER BY EFF_DATE
        ) AS Prev_END_MONTH,
        LEAD(FORMAT(TRY_CONVERT(DATE, EFF_DATE), 'yyyy-MM')) OVER (
            PARTITION BY STUDY_ID, POSTAL_CODE, STREET_LINE
            ORDER BY EFF_DATE
        ) AS Next_EFF_MONTH,
        -- Convert EFF_MONTH and Prev_END_MONTH to dates
        CAST(FORMAT(TRY_CONVERT(DATE, EFF_DATE), 'yyyy-MM') + '-01' AS DATE) AS EFF_MONTH_DATE,
        CAST(LAG(FORMAT(TRY_CONVERT(DATE, END_DATE), 'yyyy-MM')) OVER (
            PARTITION BY STUDY_ID, POSTAL_CODE, STREET_LINE
            ORDER BY EFF_DATE
        ) + '-01' AS DATE) AS Prev_END_MONTH_DATE
    FROM [HealthFiles_test].[dev].[FCT_COMBINED_HEALTH_CLIENT_TABLE]
),

Groupings AS (
    SELECT
        STUDY_ID,
        POSTAL_CODE,
        STREET_LINE,
        STREET_FIRST_TWO_WORDS,
        EFF_DATE,
        END_DATE,
        Prev_END_DATE,
        EFF_MONTH,
        END_MONTH,
        EFF_MONTH_DATE
        Next_EFF_MONTH,
        Prev_END_MONTH,
        Prev_END_MONTH_DATE,
        CASE 
            WHEN END_MONTH >= Next_EFF_MONTH THEN FORMAT(DATEADD(MONTH, -1, CAST(Next_EFF_MONTH + '-01' AS DATE)), 'yyyy-MM')
            ELSE END_MONTH
        END AS Adjusted_END_MONTH,
        -- SUM(CASE 
        --     WHEN Prev_END_MONTH IS NULL OR CAST(EFF_MONTH + '-01' AS DATE) > DATEADD(MONTH, 1, CAST(Prev_END_MONTH + '-01' AS DATE))
        --     THEN 1 ELSE 0 
        -- END) OVER (PARTITION BY STUDY_ID, POSTAL_CODE, STREET_LINE ORDER BY EFF_MONTH) AS GroupKeyMonth,
                -- Create a grouping key based on gaps in time
        SUM(CASE WHEN Prev_END_DATE IS NULL OR EFF_DATE > DATEADD(DAY, 1, Prev_END_DATE) THEN 1 ELSE 0 END)
            OVER (PARTITION BY STUDY_ID, POSTAL_CODE, STREET_LINE ORDER BY EFF_DATE) AS GroupKeyDay,
  -- Create a grouping key based on gaps in months
        SUM(CASE 
            WHEN Prev_END_MONTH_DATE IS NULL OR EFF_MONTH_DATE > DATEADD(MONTH, 1, Prev_END_MONTH_DATE)
            THEN 1 ELSE 0 
        END) OVER (PARTITION BY STUDY_ID, POSTAL_CODE, STREET_LINE ORDER BY EFF_MONTH_DATE) AS GroupKeyMonth
    FROM CleanedData
) ,
AggregatedPeriods AS (
    SELECT
        STUDY_ID,
        POSTAL_CODE,
        STREET_LINE, 
        GroupKeyMonth,
        MIN(EFF_MONTH) AS StartMonth, -- Minimum effective month
        MAX(END_MONTH) AS EndMonth -- Maximum end month
    FROM Groupings
    GROUP BY STUDY_ID, POSTAL_CODE, STREET_LINE, GroupKeyMonth
)
SELECT *
   --  STUDY_ID,
   --  POSTAL_CODE,
   --  STREET_LINE,
   --  StartMonth AS EFF_MONTH,
   --  EndMonth AS END_MONTH
   --  INTO DEV.FCT_HEALTH_CLIENT_ADDRESS_ORDERED_MONTH
FROM  AggregatedPeriods
WHERE STUDY_ID = '001A9042FA9A48CC00E28DD031448F25B9CE08AA676282D6'
ORDER BY STUDY_ID, StartMonth;






