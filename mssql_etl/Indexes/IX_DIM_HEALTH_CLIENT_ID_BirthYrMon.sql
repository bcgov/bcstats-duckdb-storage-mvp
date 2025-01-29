-- =============================================
-- Step 2: Create Non-Clustered Indexes (If Necessary)
-- =============================================

-- Example: If you frequently query by BIRTH_YR_MON
CREATE NONCLUSTERED INDEX IX_DIM_HEALTH_CLIENT_ID_BirthYrMon
ON DEV.DIM_HEALTH_CLIENT_ID (BIRTH_YR_MON);
GO

PRINT 'Non-clustered index IX_DIM_HEALTH_CLIENT_ID_BirthYrMon created successfully.';
