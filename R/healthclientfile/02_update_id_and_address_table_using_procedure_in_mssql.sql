
-- =============================================
-- Step 1: Before proceeding, confirm that the main view DEV.VIEW_COMBINED_HEALTH_CLIENT is correctly updated and contains the necessary columns:

-- STUDY_ID
-- BIRTH_YR_MON
-- SEX
-- POSTAL_CODE
-- CITY
-- STREET_LINE
-- LHA
-- CHSA
-- LATITUDE
-- LONGITUDE
-- =============================================

-- =============================================
-- Step 2 & 3: Create Stored Procedure to Update ID and Address Views
-- =============================================

IF OBJECT_ID('DEV.SP_Update_HEALTH_CLIENT_Dimensions', 'P') IS NOT NULL
BEGIN
    DROP PROCEDURE DEV.SP_Update_HEALTH_CLIENT_Dimensions;
    PRINT 'Existing stored procedure DEV.SP_Update_HEALTH_CLIENT_Dimensions dropped.';
END
GO

CREATE PROCEDURE DEV.SP_Update_HEALTH_CLIENT_Dimensions
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartTime DATETIME2 = SYSDATETIME();
    DECLARE @EndTime DATETIME2;
    DECLARE @DurationSeconds FLOAT;
    DECLARE @ErrorMessage NVARCHAR(4000);

    BEGIN TRY
        -- =============================================
        -- Step 2: Update VIEW_COMBINED_HEALTH_CLIENT_ID
        -- =============================================

        PRINT 'Starting update of VIEW_COMBINED_HEALTH_CLIENT_ID...';

        DECLARE @sql_id NVARCHAR(MAX) = N'
            CREATE OR ALTER VIEW DEV.VIEW_COMBINED_HEALTH_CLIENT_ID AS
            SELECT DISTINCT 
                [STUDY_ID], 
                [BIRTH_YR_MON], 
                [SEX]
            FROM DEV.VIEW_COMBINED_HEALTH_CLIENT;
        ';

        EXEC sp_executesql @sql_id;

        PRINT 'VIEW_COMBINED_HEALTH_CLIENT_ID updated successfully.';

        -- =============================================
        -- Step 3: Update VIEW_COMBINED_HEALTH_CLIENT_ADDRESS
        -- =============================================

        PRINT 'Starting update of VIEW_COMBINED_HEALTH_CLIENT_ADDRESS...';

        DECLARE @sql_addr NVARCHAR(MAX) = N'
            CREATE OR ALTER VIEW DEV.VIEW_COMBINED_HEALTH_CLIENT_ADDRESS AS
            SELECT 
                [POSTAL_CODE], 
                [STREET_LINE], 
                [CITY],
                MAX([LATITUDE]) AS [LATITUDE],
                MAX([LONGITUDE]) AS [LONGITUDE],
                MAX([CHSA]) AS [CHSA],
                MAX([LHA]) AS [LHA]
            FROM DEV.VIEW_COMBINED_HEALTH_CLIENT
            GROUP BY 
                [POSTAL_CODE], 
                [STREET_LINE], 
                [CITY];
        ';

        EXEC sp_executesql @sql_addr;

        PRINT 'VIEW_COMBINED_HEALTH_CLIENT_ADDRESS updated successfully.';

        -- =============================================
        -- Logging Execution Duration
        -- =============================================

        SET @EndTime = SYSDATETIME();
        SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Stored procedure DEV.SP_Update_HEALTH_CLIENT_Dimensions completed successfully in ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

    END TRY
    BEGIN CATCH
        -- Capture error details
        SET @ErrorMessage = ERROR_MESSAGE();
        SET @EndTime = SYSDATETIME();
        SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);

        -- Rollback is not necessary here as we're not using explicit transactions
        PRINT 'An error occurred while updating views: ' + @ErrorMessage;
        PRINT 'Stored procedure DEV.SP_Update_HEALTH_CLIENT_Dimensions failed after ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';
    END CATCH
END
GO

PRINT 'Stored procedure DEV.SP_Update_HEALTH_CLIENT_Dimensions created successfully.';


EXEC DEV.SP_Update_HEALTH_CLIENT_Dimensions;

-- =============================================
-- Step 4: Create Stored Procedure to Update DIM_HEALTH_CLIENT_ID
-- =============================================

IF OBJECT_ID('DEV.SP_Update_DIM_HEALTH_CLIENT_ID', 'P') IS NOT NULL
BEGIN
    DROP PROCEDURE DEV.SP_Update_DIM_HEALTH_CLIENT_ID;
    PRINT 'Existing stored procedure DEV.SP_Update_DIM_HEALTH_CLIENT_ID dropped.';
END
GO

CREATE PROCEDURE DEV.SP_Update_DIM_HEALTH_CLIENT_ID
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @StartTime DATETIME2, @EndTime DATETIME2, @DurationSeconds FLOAT;
    
    BEGIN TRY
        SET @StartTime = SYSDATETIME();
        PRINT 'Starting incremental update of DEV.DIM_HEALTH_CLIENT_ID at ' + CAST(@StartTime AS NVARCHAR);
        
        BEGIN TRANSACTION;
        
        -- MERGE statement to handle inserts and updates
        MERGE DEV.DIM_HEALTH_CLIENT_ID AS target
        USING (
            SELECT DISTINCT [STUDY_ID], [BIRTH_YR_MON], [SEX]
            FROM DEV.VIEW_COMBINED_HEALTH_CLIENT_ID
        ) AS source
        ON target.STUDY_ID = source.STUDY_ID
        WHEN MATCHED AND (
            target.BIRTH_YR_MON <> source.BIRTH_YR_MON 
            OR target.SEX <> source.SEX
        ) THEN
            UPDATE SET
                target.BIRTH_YR_MON = source.BIRTH_YR_MON,
                target.SEX = source.SEX
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (STUDY_ID, BIRTH_YR_MON, SEX)
            VALUES (source.STUDY_ID, source.BIRTH_YR_MON, source.SEX);
        
        COMMIT TRANSACTION;
        
        SET @EndTime = SYSDATETIME();
        SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Incremental update completed successfully in ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        PRINT 'An error occurred during the incremental update.';
        PRINT ERROR_MESSAGE();
    END CATCH
END
GO

PRINT 'Stored procedure DEV.SP_Update_DIM_HEALTH_CLIENT_ID created successfully.';

EXEC DEV.SP_Update_DIM_HEALTH_CLIENT_ID;


-- =============================================
-- Step 2: Create Stored Procedure to Update DIM_HEALTH_CLIENT_ADDRESS
-- =============================================

IF OBJECT_ID('DEV.SP_Update_DIM_HEALTH_CLIENT_ADDRESS', 'P') IS NOT NULL
BEGIN
    DROP PROCEDURE DEV.SP_Update_DIM_HEALTH_CLIENT_ADDRESS;
    PRINT 'Existing stored procedure DEV.SP_Update_DIM_HEALTH_CLIENT_ADDRESS dropped.';
END
GO

CREATE PROCEDURE DEV.SP_Update_DIM_HEALTH_CLIENT_ADDRESS
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartTime DATETIME2 = SYSDATETIME();
    DECLARE @EndTime DATETIME2;
    DECLARE @DurationSeconds FLOAT;
    DECLARE @ErrorMessage NVARCHAR(4000);

    BEGIN TRY
        PRINT 'Starting incremental update of DEV.DIM_HEALTH_CLIENT_ADDRESS at ' + CAST(@StartTime AS NVARCHAR);

        BEGIN TRANSACTION;

        -- MERGE statement to handle inserts and updates
        MERGE DEV.DIM_HEALTH_CLIENT_ADDRESS AS target
        USING (
            SELECT 
                [POSTAL_CODE], 
                [STREET_LINE], 
                [CITY],
                [LATITUDE],
                [LONGITUDE],
                [CHSA],
                [LHA]
            FROM DEV.VIEW_COMBINED_HEALTH_CLIENT_ADDRESS
        ) AS source
        ON 
            target.POSTAL_CODE = source.POSTAL_CODE AND
            target.STREET_LINE = source.STREET_LINE AND
            target.CITY = source.CITY

        WHEN MATCHED AND (
            ISNULL(target.LATITUDE, 0) <> ISNULL(source.LATITUDE, 0) OR
            ISNULL(target.LONGITUDE, 0) <> ISNULL(source.LONGITUDE, 0) OR
            ISNULL(target.CHSA, '') <> ISNULL(source.CHSA, '') OR
            ISNULL(target.LHA, '') <> ISNULL(source.LHA, '')
        ) THEN
            UPDATE SET
                target.LATITUDE = source.LATITUDE,
                target.LONGITUDE = source.LONGITUDE,
                target.CHSA = source.CHSA,
                target.LHA = source.LHA

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (POSTAL_CODE, STREET_LINE, CITY, LATITUDE, LONGITUDE, CHSA, LHA)
            VALUES (source.POSTAL_CODE, source.STREET_LINE, source.CITY, source.LATITUDE, source.LONGITUDE, source.CHSA, source.LHA);

        COMMIT TRANSACTION;

        SET @EndTime = SYSDATETIME();
        SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Incremental update of DEV.DIM_HEALTH_CLIENT_ADDRESS completed successfully in ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        SET @ErrorMessage = ERROR_MESSAGE();
        SET @EndTime = SYSDATETIME();
        SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);

        PRINT 'An error occurred during the incremental update of DEV.DIM_HEALTH_CLIENT_ADDRESS.';
        PRINT 'Error Message: ' + @ErrorMessage;
        PRINT 'Procedure failed after ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';
    END CATCH
END
GO

PRINT 'Stored procedure DEV.SP_Update_DIM_HEALTH_CLIENT_ADDRESS created successfully.';

EXEC DEV.SP_Update_DIM_HEALTH_CLIENT_ADDRESS;
