-- =============================================
-- Step 4b: Create Stored Procedure with Batch Processing
-- =============================================

IF OBJECT_ID('DEV.SP_Update_DIM_HEALTH_CLIENT_ID_Batch', 'P') IS NOT NULL
BEGIN
    DROP PROCEDURE DEV.SP_Update_DIM_HEALTH_CLIENT_ID_Batch;
    PRINT 'Existing stored procedure DEV.SP_Update_DIM_HEALTH_CLIENT_ID_Batch dropped.';
END
GO

CREATE PROCEDURE DEV.SP_Update_DIM_HEALTH_CLIENT_ID_Batch
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @StartTime DATETIME2, @EndTime DATETIME2, @DurationSeconds FLOAT;
    DECLARE @BatchSize INT = 1000; -- Adjust as needed
    DECLARE @ProcessedCount INT = 0;
    DECLARE @TotalCount INT;
    
    -- Temporary table to hold distinct STUDY_IDs
    IF OBJECT_ID('tempdb..#StudyIDList') IS NOT NULL DROP TABLE #StudyIDList;
    
    SELECT DISTINCT [STUDY_ID], [BIRTH_YR_MON], [SEX]
    INTO #StudyIDList
    FROM DEV.VIEW_COMBINED_HEALTH_CLIENT_ID;
    
    SELECT @TotalCount = COUNT(*) FROM #StudyIDList;
    
    PRINT 'Total STUDY_IDs to process: ' + CAST(@TotalCount AS NVARCHAR);
    
    WHILE @ProcessedCount < @TotalCount
    BEGIN
        BEGIN TRY
            BEGIN TRANSACTION;
            
            SET @StartTime = SYSDATETIME();
            
            -- Select the next batch
            WITH CTE_Batch AS (
                SELECT TOP (@BatchSize) [STUDY_ID], [BIRTH_YR_MON], [SEX]
                FROM #StudyIDList
                WHERE [STUDY_ID] NOT IN (
                    SELECT TOP (@ProcessedCount) [STUDY_ID]
                    FROM #StudyIDList
                )
                ORDER BY [STUDY_ID]
            )
            MERGE DEV.DIM_HEALTH_CLIENT_ID AS target
            USING CTE_Batch AS source
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
            
            SET @EndTime = SYSDATETIME();
            SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
            SET @ProcessedCount = @ProcessedCount + @BatchSize;
            
            PRINT 'Processed ' + CAST(@ProcessedCount AS NVARCHAR) + ' out of ' + CAST(@TotalCount AS NVARCHAR) + ' STUDY_IDs. Batch Duration: ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';
            
            COMMIT TRANSACTION;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0
                ROLLBACK TRANSACTION;
            
            PRINT 'An error occurred during batch processing.';
            PRINT ERROR_MESSAGE();
            BREAK;
        END CATCH
    END
    
    PRINT 'Batch incremental update of DEV.DIM_HEALTH_CLIENT_ID completed successfully.';
END
GO

PRINT 'Stored procedure DEV.SP_Update_DIM_HEALTH_CLIENT_ID_Batch created successfully.';
