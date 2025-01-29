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