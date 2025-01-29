CREATE INDEX IDX_MT_FCT_CombineHealth_Client
ON dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE (STUDY_ID, EFF_DATE, POSTAL_CODE, STREET_LINE);
GO
PRINT 'Composite index IDX_MT_FCT_CombineHealth_Client created successfully.';
