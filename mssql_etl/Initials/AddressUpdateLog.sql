-- Create Logging Table
IF OBJECT_ID('DEV.AddressUpdateLog', 'U') IS NULL
BEGIN
    CREATE TABLE DEV.AddressUpdateLog (
        LogID INT IDENTITY(1,1) PRIMARY KEY,
        ExecutionTime DATETIME2 DEFAULT SYSDATETIME(),
        Status NVARCHAR(50),
        DurationSeconds FLOAT,
        ErrorMessage NVARCHAR(MAX) NULL
    );
    PRINT 'Table DEV.AddressUpdateLog created successfully.';
END
GO
