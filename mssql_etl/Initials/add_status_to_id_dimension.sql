IF COL_LENGTH('dev.DIM_HEALTH_CLIENT_ID', 'Status') IS NULL
BEGIN
    ALTER TABLE dev.DIM_HEALTH_CLIENT_ID
    ADD Status VARCHAR(20) NOT NULL DEFAULT 'Pending';
    PRINT 'Column Status added successfully.';
END
ELSE
BEGIN
    PRINT 'Column Status already exists in dev.DIM_HEALTH_CLIENT_ID.';
END
