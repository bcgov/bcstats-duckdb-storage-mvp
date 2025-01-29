-- =============================================
-- Step 4: Create Stored Procedure for Batch Incremental Refresh
-- =============================================

IF OBJECT_ID('dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_BatchIncrementalRefresh', 'P') IS NOT NULL
    DROP PROCEDURE dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_BatchIncrementalRefresh;
GO

CREATE PROCEDURE dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_BatchIncrementalRefresh
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @BatchSize INT = 100000;
    DECLARE @RowsInserted INT = 1;

    PRINT 'Starting batch incremental refresh of dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE...';

    WHILE (@RowsInserted > 0)
    BEGIN
        BEGIN TRY
            BEGIN TRANSACTION;

            -- Insert a batch of new records
            INSERT INTO dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE (
                effective_year,
                effective_month,
                effective_day,
                STUDY_ID,
                POSTAL_CODE,
                CITY,
                STREET_LINE,
                LHA,
                CHSA,
                LATITUDE,
                LONGITUDE,
                EFF_DATE,
                END_DATE
            )
            SELECT TOP (@BatchSize)
                v.effective_year,
                v.effective_month,
                v.effective_day,
                v.STUDY_ID,
                v.POSTAL_CODE,
                v.CITY,
                v.STREET_LINE,
                v.LHA,
                v.CHSA,
                v.LATITUDE,
                v.LONGITUDE,
                v.EFF_DATE,
                v.END_DATE
            FROM dev.VIEW_FCT_COMBINED_HEALTH_TABLE v
            LEFT JOIN dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE mt
                ON v.STUDY_ID = mt.STUDY_ID
                AND v.EFF_DATE = mt.EFF_DATE
                AND v.POSTAL_CODE = mt.POSTAL_CODE
                AND v.STREET_LINE = mt.STREET_LINE
            WHERE mt.STUDY_ID IS NULL;

            SET @RowsInserted = @@ROWCOUNT;

            COMMIT TRANSACTION;

            PRINT CAST(@RowsInserted AS NVARCHAR(20)) + ' rows inserted in this batch.';
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0
                ROLLBACK TRANSACTION;

            PRINT 'An error occurred during batch incremental refresh.';
            PRINT ERROR_MESSAGE();

            -- Exit the loop in case of error
            BREAK;
        END CATCH
    END

    PRINT 'Batch incremental refresh completed.';
END
GO

PRINT 'Stored Procedure dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_BatchIncrementalRefresh created successfully.';
