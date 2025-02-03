-- =============================================
-- Step 2: Create Non-Clustered Indexes (If Necessary)
-- =============================================

-- Example: If you frequently query by BIRTH_YR_MON
CREATE NONCLUSTERED INDEX IX_DIM_HEALTH_CLIENT_ID_STUDY_ID
ON DEV.DIM_HEALTH_CLIENT_ID (STUDY_ID);
GO

PRINT 'Non-clustered index IX_DIM_HEALTH_CLIENT_ID_STUDY_ID created successfully.';
