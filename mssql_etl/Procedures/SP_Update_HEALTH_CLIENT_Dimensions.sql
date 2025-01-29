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
