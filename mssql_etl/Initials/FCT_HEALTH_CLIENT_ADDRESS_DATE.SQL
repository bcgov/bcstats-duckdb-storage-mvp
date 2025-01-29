CREATE TABLE [dev].[FCT_HEALTH_CLIENT_ADDRESS_DATE] (
    [STUDY_ID]               NVARCHAR (255) NULL,
    [POSTAL_CODE]            NVARCHAR (20)  NULL,
    [STREET_LINE]            NVARCHAR (255) NULL,
    [CITY]                   NVARCHAR (100) NULL,
    [STREET_FIRST_TWO_WORDS] NVARCHAR (255) NULL,
    [EFF_DATE]               DATE           NULL,
    [END_DATE]               DATE           NULL,
    [GroupAddressKey]        BIGINT         NULL,
    [LATITUDE]               DECIMAL (18)   NULL,
    [LONGITUDE]              DECIMAL (18)   NULL
);
