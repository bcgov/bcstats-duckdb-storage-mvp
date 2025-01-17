/****** Script for SelectTopNRows command from SSMS  ******/
SELECT  
      [CITY]
      , [POSTAL_CODE]
      ,[STREET_LINE]
      ,MAX([LATITUDE]) AS [LATITUDE]
      ,MAX([LONGITUDE]) AS [LONGITUDE]
 INTO dev.DIM_HEALTH_CLIENT_ADDRESS
  FROM [HealthFiles_test].[dev].[FCT_COMBINED_HEALTH_CLIENT_TABLE]
  GROUP BY [CITY]
      , [POSTAL_CODE]
      ,[STREET_LINE]