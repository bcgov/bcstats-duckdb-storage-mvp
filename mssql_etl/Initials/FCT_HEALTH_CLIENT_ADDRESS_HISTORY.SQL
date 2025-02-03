DROP TABLE DEV.FCT_HEALTH_CLIENT_ADDRESS_HISTORY;
CREATE TABLE [dev].[FCT_HEALTH_CLIENT_ADDRESS_HISTORY](
	[STUDY_ID] [nvarchar](50) ,
	[POSTAL_CODE] [nvarchar](20) NULL,
	[STREET_LINE] [nvarchar](255) NULL,
	[CITY] [nvarchar](100) NULL,	
	[EFF_DATE] [date] NULL,
	[END_DATE] [date] NULL,
	[GroupAddressKey] [bigint] NULL,
	[LATITUDE] [decimal](18, 0) NULL,
	[LONGITUDE] [decimal](18, 0) NULL
) ;