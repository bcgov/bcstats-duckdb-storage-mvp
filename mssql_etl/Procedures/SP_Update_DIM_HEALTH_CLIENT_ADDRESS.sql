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
                [LONGITUDE]
            FROM DEV.VIEW_COMBINED_HEALTH_CLIENT_ADDRESS
        ) AS source
        ON 
            target.POSTAL_CODE = source.POSTAL_CODE AND
            target.STREET_LINE = source.STREET_LINE AND
            target.CITY = source.CITY

        WHEN MATCHED AND (
            ISNULL(target.LATITUDE, 0) <> ISNULL(source.LATITUDE, 0) OR
            ISNULL(target.LONGITUDE, 0) <> ISNULL(source.LONGITUDE, 0) 
        ) THEN
            UPDATE SET
                target.LATITUDE = source.LATITUDE,
                target.LONGITUDE = source.LONGITUDE

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (POSTAL_CODE, STREET_LINE, CITY, LATITUDE, LONGITUDE)
            VALUES (source.POSTAL_CODE, source.STREET_LINE, source.CITY, source.LATITUDE, source.LONGITUDE);

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
