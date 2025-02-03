DROP TABLE DEV.CLR_EXT_20231127_TEST;
SELECT TOP (10000) [STUDY_ID]
      ,[BIRTH_YR_MON]
      ,[SEX]
      ,[POSTAL_CODE]
      ,[CITY]
      ,[STREET_LINE]
      ,[LHA]
      ,[CHSA]
      ,[LATITUDE]
      ,[LONGITUDE]
      ,[EFF_DATE]
      ,[END_DATE]
      INTO [HealthFiles_test].[dev].[CLR_EXT_20231127_TEST]
  FROM [HealthFiles_test].[dev].[CLR_EXT_20231127];

-- ALTER TABLE dev.CLR_EXT_20231127_TEST
--       ADD effective_year INT, 
--           effective_month INT,
--           effective_day INT;



-- UPDATE dev.CLR_EXT_20231127_TEST
-- SET 
--     effective_year =  2023,  
--     effective_month =  11,  
--     effective_day =  27,  
--     EFF_DATE = CASE 
--         WHEN EFF_DATE IS NULL THEN CAST('2023-10-01' AS DATE) 
--         ELSE EFF_DATE  
--     END, 
--     END_DATE = CASE 
--         WHEN END_DATE IS NULL THEN CAST('2023-10-30' AS DATE) 
--         ELSE END_DATE  
--     END;


DROP TABLE DEV.DIM_HEALTH_CLIENT_ID_TEST;
SELECT TOP (3) [STUDY_ID]
      ,[BIRTH_YR_MON]
      ,[SEX]
      INTO DEV.DIM_HEALTH_CLIENT_ID_TEST
  FROM [HealthFiles_test].[dev].[DIM_HEALTH_CLIENT_ID]