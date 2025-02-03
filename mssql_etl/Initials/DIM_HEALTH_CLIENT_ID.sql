-- =============================================
-- Step 1: Ensure DEV.DIM_HEALTH_CLIENT_ID Exists with Proper Structure
-- =============================================

IF OBJECT_ID('DEV.DIM_HEALTH_CLIENT_ID', 'U') IS NOT NULL
BEGIN
    PRINT 'Table DEV.DIM_HEALTH_CLIENT_ID already exists.';
END
ELSE
BEGIN
    CREATE TABLE DEV.DIM_HEALTH_CLIENT_ID (
        STUDY_ID NVARCHAR(50) NOT NULL PRIMARY KEY,
        BIRTH_YR_MON NVARCHAR(6) NOT NULL, -- Assuming format YYYYMM
        SEX NVARCHAR(10) NOT NULL,
        Status VARCHAR(20) NOT NULL DEFAULT 'Pending';
    );
    PRINT 'Table DEV.DIM_HEALTH_CLIENT_ID created successfully.';
END
GO
