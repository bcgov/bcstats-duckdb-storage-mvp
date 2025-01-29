-- =============================================
-- Step 1: Create Materialized Table with Surrogate Key
-- =============================================

IF OBJECT_ID('dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE', 'U') IS NOT NULL
BEGIN
    DROP TABLE dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE;
    PRINT 'Existing table dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE dropped.';
END

CREATE TABLE dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE (
    RecordID            INT IDENTITY(1,1) NOT NULL PRIMARY KEY, -- Surrogate Primary Key
    effective_year     VARCHAR(4) NOT NULL,
    effective_month    VARCHAR(2) NOT NULL,
    effective_day      VARCHAR(2) NOT NULL,
    STUDY_ID           NVARCHAR(50) NOT NULL,
    POSTAL_CODE        NVARCHAR(20)  NULL,
    CITY               NVARCHAR(100)  NULL,
    STREET_LINE        NVARCHAR(255)  NULL,
    LHA                NVARCHAR(50)  NULL,
    CHSA               NVARCHAR(50) NULL,
    LATITUDE           FLOAT NULL,
    LONGITUDE          FLOAT NULL,
    EFF_DATE           DATE NOT NULL,
    END_DATE           DATE NOT NULL
);
GO

PRINT 'Table dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE created successfully with RecordID as primary key.';

-- =============================================
-- Step 2: Create Indexes on Materialized Table
-- =============================================

CREATE NONCLUSTERED INDEX IX_MT_FCT_COMBINED_HEALTH_CLIENT_STUDYID_EFFDATE
ON dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE (STUDY_ID, EFF_DATE);
GO

PRINT 'Index IX_MT_FCT_COMBINED_HEALTH_CLIENT_STUDYID_EFFDATE created successfully.';

CREATE NONCLUSTERED INDEX IX_MT_FCT_COMBINED_HEALTH_CLIENT_POSTAL_STREET
ON dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE (POSTAL_CODE, STREET_LINE)
INCLUDE (CITY, LATITUDE, LONGITUDE, LHA, CHSA);
GO

PRINT 'Index IX_MT_FCT_COMBINED_HEALTH_CLIENT_POSTAL_STREET created successfully.';
