Absolutely, updating dimension tables incrementally is a best practice in data warehousing, ensuring data integrity, minimizing downtime, and optimizing performance. Instead of dropping and recreating the `DEV.DIM_HEALTH_CLIENT_ID` table each time, we'll implement an **incremental update** approach using the **MERGE** statement. This method will allow us to **insert new records** and **update existing ones** without affecting the entire table.

Given your existing setup and the need to handle multiple `STUDY_IDs` efficiently, we'll structure the solution as follows:

1. **Maintain the `DEV.DIM_HEALTH_CLIENT_ID` Table**:
    - Ensure the table exists with appropriate constraints.
    - Utilize a surrogate key if necessary.

2. **Create a Centralized Source View or Table**:
    - Consolidate all monthly tables into a single source for easier processing.

3. **Develop a Stored Procedure for Incremental Updates**:
    - Use the `MERGE` statement to handle inserts and updates.
    - Implement batch processing to manage large datasets.

4. **Schedule the Stored Procedure**:
    - Automate the incremental update process using SQL Server Agent or Azure Automation.

5. **Handle New Monthly Tables**:
    - Ensure that new monthly tables are seamlessly integrated into the source view or table.

Let's dive into each step in detail.

---

## **1. Maintain the `DEV.DIM_HEALTH_CLIENT_ID` Table**

### **a. Ensure Table Structure and Constraints**

First, let's ensure that the `DEV.DIM_HEALTH_CLIENT_ID` table exists with the appropriate structure and constraints. We'll assume that `STUDY_ID` is unique and serves as the primary key.

```sql
-- =============================================
-- Step 1: Ensure DEV.DIM_HEALTH_CLIENT_ID Exists with Proper Structure
-- =============================================

IF OBJECT_ID('DEV.DIM_HEALTH_CLIENT_ID', 'U') IS NOT NULL
BEGIN
    PRINT 'Table DEV.DIM_HEALTH_CLIENT_ID already exists.';
END
ELSE
BEGIN
    CREATE TABLE DEV.DIM_HEALTH_CLIENT_ID (
        STUDY_ID NVARCHAR(50) NOT NULL PRIMARY KEY,
        BIRTH_YR_MON NVARCHAR(6) NOT NULL, -- Assuming format YYYYMM
        SEX NVARCHAR(10) NOT NULL
    );
    PRINT 'Table DEV.DIM_HEALTH_CLIENT_ID created successfully.';
END
GO
```

**Explanation:**

- **Primary Key**: `STUDY_ID` is set as the primary key to ensure uniqueness.
- **Data Types**: Adjust `BIRTH_YR_MON` and `SEX` data types as per your actual data requirements.

### **b. Indexing**

Since `STUDY_ID` is the primary key, it will automatically have a clustered index. If you frequently query based on other columns, consider adding non-clustered indexes.

```sql
-- =============================================
-- Step 2: Create Non-Clustered Indexes (If Necessary)
-- =============================================

-- Example: If you frequently query by BIRTH_YR_MON
CREATE NONCLUSTERED INDEX IX_DIM_HEALTH_CLIENT_ID_BirthYrMon
ON DEV.DIM_HEALTH_CLIENT_ID (BIRTH_YR_MON);
GO

PRINT 'Non-clustered index IX_DIM_HEALTH_CLIENT_ID_BirthYrMon created successfully.';
```

**Note:** Adjust or add indexes based on your query patterns to optimize performance.

---

## **2. Create a Centralized Source View or Table**

To simplify the processing, especially as new monthly tables are added, it's beneficial to have a **centralized view** that aggregates all monthly data. This way, the stored procedure can reference a single source.

### **a. Create a Centralized View**

```sql
-- =============================================
-- Step 3: Create Centralized View COMBINED_HEALTH_CLIENT_ID
-- =============================================

IF OBJECT_ID('DEV.VIEW_COMBINED_HEALTH_CLIENT_ID', 'V') IS NOT NULL
BEGIN
    DROP VIEW DEV.VIEW_COMBINED_HEALTH_CLIENT_ID;
    PRINT 'Existing view DEV.VIEW_COMBINED_HEALTH_CLIENT_ID dropped.';
END

CREATE VIEW DEV.VIEW_COMBINED_HEALTH_CLIENT_ID AS
WITH COMBINED_HEALTH_TABLE AS (
    -- Include all your UNION ALL statements here
    SELECT  '2020' AS effective_year, '02' AS effective_month, '13' AS effective_day,
            [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
            [LHA],  NULL AS [CHSA], NULL AS [LATITUDE], NULL AS [LONGITUDE],
              '2020-01-01' AS [EFF_DATE],
              '2020-01-31' AS [END_DATE]
     FROM dev.CLR_EXT_20200213_for_201107
    UNION ALL
    -- Add all other UNION ALL statements here...
    SELECT  '2022' AS effective_year, '09' AS effective_month, '26' AS effective_day,
            [STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE],
            [LHA],  [CHSA], [LATITUDE], [LONGITUDE],
              ISNULL([EFF_DATE], '2024-08-01') AS [EFF_DATE],
              ISNULL([END_DATE], '2024-08-31') AS [END_DATE]
     FROM dev.bc_stat_population_estimates_20240926
)
SELECT DISTINCT [STUDY_ID], [BIRTH_YR_MON], [SEX]
FROM COMBINED_HEALTH_TABLE;

GO

PRINT 'View DEV.VIEW_COMBINED_HEALTH_CLIENT_ID created successfully.';
```

**Explanation:**

- **Centralized View**: `DEV.VIEW_COMBINED_HEALTH_CLIENT_ID` consolidates all monthly tables.
- **Distinct Selection**: Ensures that duplicate `STUDY_IDs` are handled appropriately.

**Maintenance Tip:** Whenever a new monthly table is added, update this view by adding a new `UNION ALL` statement.

**Alternative Approach:** Instead of manually adding `UNION ALL` statements, consider using dynamic SQL or automation scripts to update the view when new tables are introduced.

---

## **3. Develop a Stored Procedure for Incremental Updates**

We'll create a stored procedure that performs the following:

1. **Extracts distinct `STUDY_ID`, `BIRTH_YR_MON`, and `SEX`** from the centralized view.
2. **Merges** this data into the `DEV.DIM_HEALTH_CLIENT_ID` table:
    - **Inserts** new `STUDY_IDs`.
    - **Updates** existing records if `BIRTH_YR_MON` or `SEX` have changed.

### **a. Create the Stored Procedure**

```sql
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
```

**Explanation:**

- **MERGE Statement**:
    - **Matching Criteria**: Based on `STUDY_ID`.
    - **When Matched**: If `BIRTH_YR_MON` or `SEX` differ, update the target table.
    - **When Not Matched**: Insert new `STUDY_IDs`.

- **Transaction Management**: Ensures atomicity. If any part fails, changes are rolled back.

- **Error Handling**: Catches and logs errors without crashing the entire process.

### **b. Batch Processing Consideration**

Given that `DIM_HEALTH_CLIENT_ID` is likely a smaller dimension table compared to your fact tables, batch processing may not be necessary. However, if you anticipate a very large number of `STUDY_IDs`, you can implement batch processing similarly to how you handled the fact table.

**Example of Batch Processing (Optional):**

If you decide to implement batch processing, here's how you can modify the stored procedure:

```sql
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
```

**Explanation:**

- **Batch Size**: Processes records in chunks (`@BatchSize`) to manage memory and transaction log usage.
- **Looping Mechanism**: Continues until all `STUDY_IDs` are processed.
- **MERGE within Each Batch**: Performs insertions and updates for each batch separately.
- **Progress Logging**: Outputs progress and batch duration for monitoring.

**Note:** Adjust `@BatchSize` based on your system's performance and the size of the dimension table.

---

## **4. Schedule the Stored Procedure**

Automate the execution of the stored procedure to ensure that the dimension table is updated regularly, especially after new monthly data is loaded.

### **a. Using SQL Server Agent**

1. **Open SQL Server Management Studio (SSMS)** and connect to your SQL Server instance.

2. **Navigate to SQL Server Agent**:
    - Expand the **SQL Server Agent** node.
    - Ensure that the SQL Server Agent service is running.

3. **Create a New Job**:
    - Right-click on **Jobs** and select **New Job**.

4. **Configure the Job**:
    - **General Tab**:
        - **Name**: `Incremental Update of DIM_HEALTH_CLIENT_ID`
        - **Description**: `Updates the DIM_HEALTH_CLIENT_ID dimension table with new STUDY_IDs and updates existing records as necessary.`
    
    - **Steps Tab**:
        - Click **New** to create a new step.
        - **Step Name**: `Execute SP_Update_DIM_HEALTH_CLIENT_ID`
        - **Type**: `Transact-SQL script (T-SQL)`
        - **Database**: Select your database.
        - **Command**:
            ```sql
            EXEC DEV.SP_Update_DIM_HEALTH_CLIENT_ID;
            ```
            *Or, if using batch processing:*
            ```sql
            EXEC DEV.SP_Update_DIM_HEALTH_CLIENT_ID_Batch;
            ```
    
    - **Schedules Tab**:
        - Click **New** to create a schedule.
        - **Name**: `Monthly DIM_HEALTH_CLIENT_ID Update`
        - **Schedule Type**: `Recurring`
        - **Frequency**: Set to run after the monthly data load completes (e.g., first day of each month).
        - **Daily Frequency**: Set an appropriate time (e.g., 1:00 AM).
    
    - **Alerts and Notifications** (Optional):
        - Configure alerts to notify administrators in case of job failures.
    
5. **Save the Job**:
    - Click **OK** to save the job.

**b. Verification**

After setting up the job, manually execute it once to ensure that it functions as expected without errors.

---

## **5. Handle New Monthly Tables**

To ensure that new monthly tables are seamlessly integrated into the centralized view, consider the following strategies:

### **a. Automate View Updates**

Instead of manually adding `UNION ALL` statements for each new monthly table, automate the process using dynamic SQL or metadata queries.

**Example: Dynamic SQL to Generate the View**

```sql
-- =============================================
-- Step 5: Automate View Updates with Dynamic SQL
-- =============================================

DECLARE @sql NVARCHAR(MAX) = N'CREATE OR ALTER VIEW DEV.VIEW_COMBINED_HEALTH_CLIENT_ID AS
WITH COMBINED_HEALTH_TABLE AS (
';

-- Append each CLR_EXT_YYYYMMDD table
SELECT @sql += 
    'SELECT ''' + 
    FORMAT([effective_year] AS VARCHAR(4)) + ''' AS effective_year, ''' +
    FORMAT([effective_month] AS VARCHAR(2)) + ''' AS effective_month, ''' +
    FORMAT([effective_day] AS VARCHAR(2)) + ''' AS effective_day, ' +
    '[STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE], ' +
    '[LHA], [CHSA], [LATITUDE], [LONGITUDE], ' +
    'ISNULL([EFF_DATE], ''' + CAST([default_eff_date] AS VARCHAR) + ''') AS [EFF_DATE], ' +
    'ISNULL([END_DATE], ''' + CAST([default_end_date] AS VARCHAR) + ''') AS [END_DATE] ' +
    'FROM ' + QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME) + ' ' +
    'UNION ALL '
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'dev' AND TABLE_NAME LIKE 'CLR_EXT_%';

-- Remove the last 'UNION ALL '
SET @sql = LEFT(@sql, LEN(@sql) - LEN(' UNION ALL ')) + ') ' +
'SELECT DISTINCT [STUDY_ID], [BIRTH_YR_MON], [SEX] ' +
'FROM COMBINED_HEALTH_TABLE;';

-- Execute the dynamic SQL
EXEC sp_executesql @sql;

PRINT 'View DEV.VIEW_COMBINED_HEALTH_CLIENT_ID updated successfully with new monthly tables.';
```

**Explanation:**

- **Dynamic Generation**: Automatically constructs the `UNION ALL` statements based on existing tables matching the pattern `CLR_EXT_%`.
- **Default Dates**: Adjust `default_eff_date` and `default_end_date` as per your requirements or include them as columns in a metadata table.
- **Automate Execution**: Schedule this script to run whenever a new monthly table is added, ensuring the view is always up-to-date.

### **b. Naming Conventions**

Ensure that all monthly tables follow a consistent naming convention (e.g., `CLR_EXT_YYYYMMDD`) to facilitate automated processes.

---

## **6. Comprehensive Refactored Solution**

Combining all the steps, here's a summary of the refactored approach:

1. **Maintain `DEV.DIM_HEALTH_CLIENT_ID` Table**:
    - Ensure it exists with the correct structure and constraints.
    - Add necessary indexes.

2. **Create a Centralized View**:
    - Consolidate all monthly tables into `DEV.VIEW_COMBINED_HEALTH_CLIENT_ID`.
    - Automate the update of this view when new monthly tables are added.

3. **Develop and Schedule a Stored Procedure**:
    - `DEV.SP_Update_DIM_HEALTH_CLIENT_ID` handles incremental inserts and updates.
    - Optionally, use batch processing with `DEV.SP_Update_DIM_HEALTH_CLIENT_ID_Batch` for very large datasets.
    - Schedule the stored procedure to run monthly using SQL Server Agent.

4. **Automate Integration of New Monthly Tables**:
    - Use dynamic SQL to update the centralized view automatically.
    - Ensure naming conventions are adhered to for seamless automation.

---

## **7. Example: Complete Implementation**

### **a. Centralized View with Dynamic SQL**

To automate the inclusion of new monthly tables, create a separate stored procedure that updates the centralized view.

```sql
-- =============================================
-- Step 5a: Create Stored Procedure to Update Centralized View
-- =============================================

IF OBJECT_ID('DEV.SP_Update_COMBINED_HEALTH_CLIENT_ID_View', 'P') IS NOT NULL
BEGIN
    DROP PROCEDURE DEV.SP_Update_COMBINED_HEALTH_CLIENT_ID_View;
    PRINT 'Existing stored procedure DEV.SP_Update_COMBINED_HEALTH_CLIENT_ID_View dropped.';
END
GO

CREATE PROCEDURE DEV.SP_Update_COMBINED_HEALTH_CLIENT_ID_View
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @sql NVARCHAR(MAX) = N'CREATE OR ALTER VIEW DEV.VIEW_COMBINED_HEALTH_CLIENT_ID AS
WITH COMBINED_HEALTH_TABLE AS (
';
    
    -- Append each CLR_EXT_YYYYMMDD table
    SELECT @sql += 
        'SELECT ''' + 
        CAST(YEAR([EFF_DATE]) AS VARCHAR(4)) + ''' AS effective_year, ''' +
        RIGHT('0' + CAST(MONTH([EFF_DATE]) AS VARCHAR(2)), 2) + ''' AS effective_month, ''' +
        RIGHT('0' + CAST(DAY([EFF_DATE]) AS VARCHAR(2)), 2) + ''' AS effective_day, ' +
        '[STUDY_ID], [BIRTH_YR_MON], [SEX], [POSTAL_CODE], [CITY], [STREET_LINE], ' +
        '[LHA], [CHSA], [LATITUDE], [LONGITUDE], ' +
        'ISNULL([EFF_DATE], ''' + CAST([EFF_DATE] AS VARCHAR) + ''') AS [EFF_DATE], ' +
        'ISNULL([END_DATE], ''' + CAST([END_DATE] AS VARCHAR) + ''') AS [END_DATE] ' +
        'FROM ' + QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME) + ' ' +
        'UNION ALL '
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'dev' AND TABLE_NAME LIKE 'CLR_EXT_%';
    
    -- Remove the last 'UNION ALL '
    IF LEN(@sql) > 0
    BEGIN
        SET @sql = LEFT(@sql, LEN(@sql) - LEN(' UNION ALL ')) + ') ' +
        'SELECT DISTINCT [STUDY_ID], [BIRTH_YR_MON], [SEX] ' +
        'FROM COMBINED_HEALTH_TABLE;';
    END
    ELSE
    BEGIN
        SET @sql += '
        SELECT DISTINCT [STUDY_ID], [BIRTH_YR_MON], [SEX]
        FROM COMBINED_HEALTH_TABLE;';
    END
    
    -- Execute the dynamic SQL
    EXEC sp_executesql @sql;
    
    PRINT 'View DEV.VIEW_COMBINED_HEALTH_CLIENT_ID updated successfully.';
END
GO

PRINT 'Stored procedure DEV.SP_Update_COMBINED_HEALTH_CLIENT_ID_View created successfully.';
```

**Usage:**

- After adding a new monthly table (e.g., `CLR_EXT_20240128`), execute the stored procedure to update the view:
    ```sql
    EXEC DEV.SP_Update_COMBINED_HEALTH_CLIENT_ID_View;
    ```

- **Automation Tip:** Schedule this stored procedure to run immediately after a new monthly table is loaded, ensuring the view is always current.

### **b. Stored Procedure for Incremental Updates**

Here's the refined stored procedure incorporating the central view:

```sql
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
```

**Explanation:**

- **Centralized Source**: References `DEV.VIEW_COMBINED_HEALTH_CLIENT_ID` for data consistency.
- **MERGE Logic**: Inserts new `STUDY_IDs` and updates existing ones if attributes have changed.
- **Performance**: For large datasets, consider the batch version or optimizing indexes further.

**Optional: Batch Processing Version**

If you opt for batch processing due to an exceptionally large number of `STUDY_IDs`, use the previously provided `SP_Update_DIM_HEALTH_CLIENT_ID_Batch` stored procedure.

---

## **8. Example Usage Scenario**

Assuming you've added a new monthly table `CLR_EXT_20240128`, here's how you'd update the dimension table:

1. **Update the Centralized View**:
    ```sql
    EXEC DEV.SP_Update_COMBINED_HEALTH_CLIENT_ID_View;
    ```

2. **Run the Incremental Update Stored Procedure**:
    ```sql
    EXEC DEV.SP_Update_DIM_HEALTH_CLIENT_ID;
    ```

3. **Verify the Update**:
    ```sql
    SELECT * FROM DEV.DIM_HEALTH_CLIENT_ID
    WHERE STUDY_ID = 'YourSpecificStudyID';
    ```

4. **Automate the Process**:
    - Ensure that both stored procedures (`SP_Update_COMBINED_HEALTH_CLIENT_ID_View` and `SP_Update_DIM_HEALTH_CLIENT_ID`) are scheduled appropriately, typically after the monthly data load.

---

## **9. Additional Best Practices**

1. **Data Quality Checks**:
    - Before merging, validate the data in your monthly tables to ensure consistency and accuracy.
    - Implement constraints or validation rules within your staging or source tables.

2. **Logging and Monitoring**:
    - Enhance stored procedures to log detailed information about each run, including the number of records inserted and updated.
    - Store logs in a dedicated logging table for auditing and troubleshooting.

3. **Error Notifications**:
    - Configure alerts to notify administrators if the stored procedures fail.
    - Utilize SQL Server Agent's notification features or integrate with email services.

4. **Performance Optimization**:
    - Regularly review and optimize indexes on both source views and dimension tables.
    - Analyze query execution plans to identify and address bottlenecks.

5. **Backup Strategy**:
    - Include dimension tables in your regular backup routines.
    - Ensure that you can restore previous states if necessary.

6. **Documentation**:
    - Document all stored procedures, views, and processes.
    - Maintain version control for scripts to track changes over time.

7. **Security and Permissions**:
    - Ensure that only authorized personnel can execute the stored procedures and modify dimension tables.
    - Implement role-based access controls to safeguard sensitive data.

---

## **10. Final Recommendations**

By transitioning to an **incremental update** approach using the **MERGE** statement and **centralized views**, you achieve:

- **Efficiency**: Only new or changed records are processed, reducing overhead.
- **Scalability**: Easily accommodate new monthly tables without significant script modifications.
- **Data Integrity**: Ensures that the dimension table remains accurate and up-to-date.
- **Maintainability**: Simplifies the process, making it easier to manage and troubleshoot.

**Next Steps:**

1. **Implement the Stored Procedures**: Create and test the stored procedures in a development environment.
2. **Validate the Process**: Ensure that the MERGE operations correctly insert and update records without data loss.
3. **Automate Execution**: Schedule the procedures to run after each monthly data load.
4. **Monitor Performance**: Regularly check the execution time and resource utilization, adjusting batch sizes if necessary.
5. **Maintain Documentation**: Keep thorough documentation of all procedures, scripts, and processes for future reference.



 To accommodate processing **multiple `STUDY_IDs`** from the `dev.DIM_HEALTH_CLIENT_ID` table and handle large datasets efficiently through **batch processing**, we'll refactor your existing script accordingly. This approach ensures scalability, maintains performance, and integrates seamlessly with your monthly data appends.

---

## **Overview of the Refactored Solution**

1. **Batch Processing Strategy**:
    - **Batch Size**: Define a manageable number of `STUDY_IDs` to process in each iteration (e.g., 100).
    - **Looping Mechanism**: Use a `WHILE` loop to process batches until all `STUDY_IDs` are handled.
  
2. **Enhanced Filtering**:
    - **Additional Join Conditions**: Incorporate `POSTAL_CODE` and `STREET_LINE` in the `LEFT JOIN` to accurately identify new records.
  
3. **Temporary Tables**:
    - **Isolation**: Use temporary tables within each batch to prevent data overlap and ensure accurate processing.
  
4. **Performance Optimizations**:
    - **Indexing**: Create indexes on temporary tables to speed up operations.
    - **Transaction Management**: Wrap each batch processing within a transaction to maintain data integrity.
  
5. **Logging and Timing**:
    - **Progress Tracking**: Print informative messages to monitor the progress and performance of each batch.

---

## **Refactored Script for Batch Processing**

Below is the comprehensive refactored script that incorporates the above strategies. This script processes `STUDY_IDs` in batches, ensuring efficient handling of large datasets and maintaining data integrity.

```sql
-- =============================================
-- Step 0: Initialize Timing Variables
-- =============================================

DECLARE @StartTime DATETIME2, @EndTime DATETIME2, @DurationSeconds FLOAT;
DECLARE @BatchSize INT = 100; -- Adjust the batch size as needed
DECLARE @ProcessedCount INT = 0;
DECLARE @TotalCount INT;

-- =============================================
-- Step 1: Initialize the StudyID List
-- =============================================

-- Create a temporary table to hold STUDY_IDs to process
IF OBJECT_ID('tempdb..#StudyIDList') IS NOT NULL DROP TABLE #StudyIDList;

SELECT STUDY_ID
INTO #StudyIDList
FROM dev.DIM_HEALTH_CLIENT_ID;

-- Get the total number of STUDY_IDs to process
SELECT @TotalCount = COUNT(*) FROM #StudyIDList;

PRINT 'Total STUDY_IDs to process: ' + CAST(@TotalCount AS NVARCHAR);

-- =============================================
-- Step 2: Batch Processing Loop
-- =============================================

WHILE EXISTS (SELECT 1 FROM #StudyIDList)
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        -- Initialize Timing for the Batch
        SET @StartTime = SYSDATETIME();

        -- Select the top @BatchSize STUDY_IDs for this batch
        SELECT TOP (@BatchSize) STUDY_ID
        INTO #CurrentBatch
        FROM #StudyIDList;

        -- =============================================
        -- Step 3: Clean and Prepare Data (Using Staging Table)
        -- =============================================

        -- Drop temporary tables if they already exist
        IF OBJECT_ID('tempdb..#CleanedData') IS NOT NULL DROP TABLE #CleanedData;

        -- Select and transform data from the staging table for the current batch
        SELECT
            STUDY_ID,
            POSTAL_CODE,
            STREET_LINE,
            -- Extract the first two words from STREET_LINE
            CASE 
                WHEN STREET_LINE IS NOT NULL THEN
                    CASE
                        WHEN CHARINDEX(' ', STREET_LINE) > 0 THEN
                            LEFT(
                                STREET_LINE,
                                CHARINDEX(' ', STREET_LINE + ' ', CHARINDEX(' ', STREET_LINE + ' ') + 1) - 1
                            )
                        ELSE STREET_LINE
                    END
                ELSE STREET_LINE
            END AS STREET_FIRST_TWO_WORDS,
            CAST(EFF_DATE AS DATE) AS EFF_DATE,
            CAST(END_DATE AS DATE) AS END_DATE
        INTO #CleanedData
        FROM [HealthFiles_test].[dev].[Staging_FCT_COMBINED_HEALTH_CLIENT_TABLE] S
        INNER JOIN #CurrentBatch C
            ON S.STUDY_ID = C.STUDY_ID;

        -- Create indexes to optimize subsequent operations
        CREATE NONCLUSTERED INDEX IX_CleanedData_EFF_DATE ON #CleanedData (STUDY_ID, EFF_DATE);
        
        -- =============================================
        -- Step 4: Compute Previous Values Using Window Functions
        -- =============================================

        -- Drop temporary tables if they already exist
        IF OBJECT_ID('tempdb..#LaggedData') IS NOT NULL DROP TABLE #LaggedData;

        -- Create and populate #LaggedData with previous postal code and street words
        SELECT
            CD.STUDY_ID,
            CD.POSTAL_CODE,
            CD.STREET_LINE,
            CD.STREET_FIRST_TWO_WORDS,
            CD.EFF_DATE,
            CD.END_DATE,
            LAG(CD.POSTAL_CODE) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE, CD.RecordID) AS Prev_POSTAL_CODE,
            LAG(CD.STREET_FIRST_TWO_WORDS) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE, CD.RecordID) AS Prev_STREET_FIRST_TWO_WORDS
        INTO #LaggedData
        FROM (
            SELECT 
                *, 
                ROW_NUMBER() OVER (PARTITION BY STUDY_ID ORDER BY EFF_DATE) AS RecordID
            FROM #CleanedData
        ) CD
        ORDER BY CD.EFF_DATE, CD.RecordID;

        -- Create index to optimize window functions
        CREATE NONCLUSTERED INDEX IX_LaggedData_EFF_DATE ON #LaggedData (EFF_DATE);

        -- =============================================
        -- Step 5: Flag Changes in Address
        -- =============================================

        -- Drop temporary tables if they already exist
        IF OBJECT_ID('tempdb..#ChangeFlagData') IS NOT NULL DROP TABLE #ChangeFlagData;

        -- Create and populate #ChangeFlagData with ChangeFlag
        SELECT
            LD.STUDY_ID,
            LD.POSTAL_CODE,
            LD.STREET_LINE,
            LD.STREET_FIRST_TWO_WORDS,
            LD.EFF_DATE,
            LD.END_DATE,
            LD.Prev_POSTAL_CODE,
            LD.Prev_STREET_FIRST_TWO_WORDS,
            CASE 
                WHEN LD.Prev_POSTAL_CODE IS NULL 
                     OR LD.Prev_STREET_FIRST_TWO_WORDS IS NULL 
                     OR LD.Prev_POSTAL_CODE != LD.POSTAL_CODE 
                     OR LD.Prev_STREET_FIRST_TWO_WORDS != LD.STREET_FIRST_TWO_WORDS 
                THEN 1 
                ELSE 0 
            END AS ChangeFlag
        INTO #ChangeFlagData
        FROM #LaggedData LD
        ORDER BY LD.EFF_DATE, LD.RecordID;

        -- Create index to optimize subsequent operations
        CREATE NONCLUSTERED INDEX IX_ChangeFlagData_EFF_DATE ON #ChangeFlagData (EFF_DATE);

        -- =============================================
        -- Step 6: Assign GroupAddressKey Using Cumulative Sum
        -- =============================================

        -- Drop temporary tables if they already exist
        IF OBJECT_ID('tempdb..#GroupedKeyData') IS NOT NULL DROP TABLE #GroupedKeyData;

        -- Create and populate #GroupedKeyData with GroupAddressKey
        SELECT
            CFD.STUDY_ID,
            CFD.POSTAL_CODE,
            CFD.STREET_LINE,
            CFD.STREET_FIRST_TWO_WORDS,
            CFD.EFF_DATE,
            CFD.END_DATE,
            CFD.Prev_POSTAL_CODE,
            CFD.Prev_STREET_FIRST_TWO_WORDS,
            CFD.ChangeFlag,
            -- Cumulative sum to assign GroupAddressKey
            SUM(CASE WHEN CFD.ChangeFlag = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY CFD.STUDY_ID ORDER BY CFD.EFF_DATE, CFD.RecordID ROWS UNBOUNDED PRECEDING) AS GroupAddressKey
        INTO #GroupedKeyData
        FROM #ChangeFlagData CFD
        ORDER BY CFD.EFF_DATE, CFD.RecordID;

        -- Create index to optimize aggregation
        CREATE NONCLUSTERED INDEX IX_GroupedKeyData_GroupAddressKey ON #GroupedKeyData (GroupAddressKey);

        -- =============================================
        -- Step 7: Aggregate Grouped Data
        -- =============================================

        -- Drop temporary tables if they already exist
        IF OBJECT_ID('tempdb..#GroupedData') IS NOT NULL DROP TABLE #GroupedData;

        -- Create and populate #GroupedData with aggregated information
        SELECT
            GK.STUDY_ID,
            GK.POSTAL_CODE,
            GK.STREET_FIRST_TWO_WORDS,
            MAX(GK.STREET_LINE) AS STREET_LINE,
            MIN(GK.EFF_DATE) AS EFF_DATE,
            MAX(GK.END_DATE) AS END_DATE,
            GK.GroupAddressKey
        INTO #GroupedData
        FROM #GroupedKeyData GK
        GROUP BY GK.STUDY_ID, GK.POSTAL_CODE, GK.STREET_FIRST_TWO_WORDS, GK.GroupAddressKey;

        -- =============================================
        -- Step 8: Insert Aggregated Data into Target Table (Optimized Join)
        -- =============================================

        -- Insert the processed data into the target table
        INSERT INTO DEV.FCT_HEALTH_CLIENT_ADDRESS_DATE (
            STUDY_ID,
            GroupAddressKey,
            POSTAL_CODE,
            STREET_LINE,
            EFF_DATE,
            END_DATE,
            CITY,
            LATITUDE,
            LONGITUDE
        )
        SELECT 
            GD.STUDY_ID,
            GD.GroupAddressKey,
            GD.POSTAL_CODE,
            GD.STREET_LINE,
            GD.EFF_DATE,
            GD.END_DATE,       
            B.[CITY], 
            B.LATITUDE, 
            B.LONGITUDE
        FROM #GroupedData GD
        LEFT JOIN dev.DIM_HEALTH_CLIENT_ADDRESS B
            ON GD.POSTAL_CODE = B.POSTAL_CODE 
            AND GD.STREET_LINE = B.STREET_LINE;

        -- =============================================
        -- Step 9: Cleanup Temporary Tables for the Batch
        -- =============================================

        DROP TABLE IF EXISTS #CleanedData, #LaggedData, #ChangeFlagData, #GroupedKeyData, #GroupedData, #CurrentBatch;

        -- Calculate Duration for the Batch
        SET @EndTime = SYSDATETIME();
        SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
        SET @ProcessedCount = @ProcessedCount + (SELECT COUNT(*) FROM #CurrentBatch);
        PRINT 'Processed ' + CAST(@ProcessedCount AS NVARCHAR) + ' out of ' + CAST(@TotalCount AS NVARCHAR) + ' STUDY_IDs. Current Batch Duration: ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT 'An error occurred during batch processing.';
        PRINT ERROR_MESSAGE();

        -- Optionally, you can decide to exit the loop or continue
        BREAK;
    END CATCH
END

PRINT 'All batches processed successfully.';
```

---

## **Detailed Explanation of the Refactored Script**

### **1. Initialize Timing and Batch Variables**

```sql
DECLARE @StartTime DATETIME2, @EndTime DATETIME2, @DurationSeconds FLOAT;
DECLARE @BatchSize INT = 100; -- Adjust the batch size as needed
DECLARE @ProcessedCount INT = 0;
DECLARE @TotalCount INT;
```

- **`@BatchSize`**: Determines how many `STUDY_IDs` are processed in each batch. Adjust based on your system's performance and resource availability.
- **`@ProcessedCount` & `@TotalCount`**: Track the progress of batch processing.

### **2. Initialize the StudyID List**

```sql
-- Create a temporary table to hold STUDY_IDs to process
IF OBJECT_ID('tempdb..#StudyIDList') IS NOT NULL DROP TABLE #StudyIDList;

SELECT STUDY_ID
INTO #StudyIDList
FROM dev.DIM_HEALTH_CLIENT_ID;

-- Get the total number of STUDY_IDs to process
SELECT @TotalCount = COUNT(*) FROM #StudyIDList;

PRINT 'Total STUDY_IDs to process: ' + CAST(@TotalCount AS NVARCHAR);
```

- **`#StudyIDList`**: Holds all `STUDY_IDs` that need to be processed.
- **`@TotalCount`**: Captures the total number of `STUDY_IDs` for progress tracking.

### **3. Batch Processing Loop**

```sql
WHILE EXISTS (SELECT 1 FROM #StudyIDList)
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        -- Initialize Timing for the Batch
        SET @StartTime = SYSDATETIME();

        -- Select the top @BatchSize STUDY_IDs for this batch
        SELECT TOP (@BatchSize) STUDY_ID
        INTO #CurrentBatch
        FROM #StudyIDList;

        -- Process the current batch
        -- Steps 3 to 9...
```

- **`WHILE` Loop**: Continues until all `STUDY_IDs` are processed.
- **`#CurrentBatch`**: Holds the subset of `STUDY_IDs` for the current iteration.
  
### **4. Clean and Prepare Data**

```sql
-- Drop temporary tables if they already exist
IF OBJECT_ID('tempdb..#CleanedData') IS NOT NULL DROP TABLE #CleanedData;

-- Select and transform data from the staging table for the current batch
SELECT
    STUDY_ID,
    POSTAL_CODE,
    STREET_LINE,
    -- Extract the first two words from STREET_LINE
    CASE 
        WHEN STREET_LINE IS NOT NULL THEN
            CASE
                WHEN CHARINDEX(' ', STREET_LINE) > 0 THEN
                    LEFT(
                        STREET_LINE,
                        CHARINDEX(' ', STREET_LINE + ' ', CHARINDEX(' ', STREET_LINE + ' ') + 1) - 1
                    )
                ELSE STREET_LINE
            END
        ELSE STREET_LINE
    END AS STREET_FIRST_TWO_WORDS,
    CAST(EFF_DATE AS DATE) AS EFF_DATE,
    CAST(END_DATE AS DATE) AS END_DATE
INTO #CleanedData
FROM [HealthFiles_test].[dev].[Staging_FCT_COMBINED_HEALTH_CLIENT_TABLE] S
INNER JOIN #CurrentBatch C
    ON S.STUDY_ID = C.STUDY_ID;

-- Create indexes to optimize subsequent operations
CREATE NONCLUSTERED INDEX IX_CleanedData_EFF_DATE ON #CleanedData (STUDY_ID, EFF_DATE);
```

- **Data Transformation**: Extracts the first two words from `STREET_LINE` and casts date fields.
- **`INNER JOIN`**: Ensures only data related to the current batch's `STUDY_IDs` is processed.
- **Indexing**: Improves performance for subsequent window functions.

### **5. Compute Previous Values Using Window Functions**

```sql
-- Drop temporary tables if they already exist
IF OBJECT_ID('tempdb..#LaggedData') IS NOT NULL DROP TABLE #LaggedData;

-- Create and populate #LaggedData with previous postal code and street words
SELECT
    CD.STUDY_ID,
    CD.POSTAL_CODE,
    CD.STREET_LINE,
    CD.STREET_FIRST_TWO_WORDS,
    CD.EFF_DATE,
    CD.END_DATE,
    LAG(CD.POSTAL_CODE) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE, CD.RecordID) AS Prev_POSTAL_CODE,
    LAG(CD.STREET_FIRST_TWO_WORDS) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE, CD.RecordID) AS Prev_STREET_FIRST_TWO_WORDS
INTO #LaggedData
FROM (
    SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY STUDY_ID ORDER BY EFF_DATE) AS RecordID
    FROM #CleanedData
) CD
ORDER BY CD.EFF_DATE, CD.RecordID;

-- Create index to optimize window functions
CREATE NONCLUSTERED INDEX IX_LaggedData_EFF_DATE ON #LaggedData (EFF_DATE);
```

- **`ROW_NUMBER()`**: Assigns a unique identifier (`RecordID`) within each `STUDY_ID` partition to handle multiple records with the same `EFF_DATE`.
- **`LAG` Functions**: Retrieves previous `POSTAL_CODE` and `STREET_FIRST_TWO_WORDS` within each `STUDY_ID`.
  
### **6. Flag Changes in Address**

```sql
-- Drop temporary tables if they already exist
IF OBJECT_ID('tempdb..#ChangeFlagData') IS NOT NULL DROP TABLE #ChangeFlagData;

-- Create and populate #ChangeFlagData with ChangeFlag
SELECT
    LD.STUDY_ID,
    LD.POSTAL_CODE,
    LD.STREET_LINE,
    LD.STREET_FIRST_TWO_WORDS,
    LD.EFF_DATE,
    LD.END_DATE,
    LD.Prev_POSTAL_CODE,
    LD.Prev_STREET_FIRST_TWO_WORDS,
    CASE 
        WHEN LD.Prev_POSTAL_CODE IS NULL 
             OR LD.Prev_STREET_FIRST_TWO_WORDS IS NULL 
             OR LD.Prev_POSTAL_CODE != LD.POSTAL_CODE 
             OR LD.Prev_STREET_FIRST_TWO_WORDS != LD.STREET_FIRST_TWO_WORDS 
        THEN 1 
        ELSE 0 
    END AS ChangeFlag
INTO #ChangeFlagData
FROM #LaggedData LD
ORDER BY LD.EFF_DATE, LD.RecordID;

-- Create index to optimize subsequent operations
CREATE NONCLUSTERED INDEX IX_ChangeFlagData_EFF_DATE ON #ChangeFlagData (EFF_DATE);
```

- **`ChangeFlag`**: Indicates whether there has been a change in `POSTAL_CODE` or `STREET_FIRST_TWO_WORDS` compared to the previous record.
  
### **7. Assign GroupAddressKey Using Cumulative Sum**

```sql
-- Drop temporary tables if they already exist
IF OBJECT_ID('tempdb..#GroupedKeyData') IS NOT NULL DROP TABLE #GroupedKeyData;

-- Create and populate #GroupedKeyData with GroupAddressKey
SELECT
    CFD.STUDY_ID,
    CFD.POSTAL_CODE,
    CFD.STREET_LINE,
    CFD.STREET_FIRST_TWO_WORDS,
    CFD.EFF_DATE,
    CFD.END_DATE,
    CFD.Prev_POSTAL_CODE,
    CFD.Prev_STREET_FIRST_TWO_WORDS,
    CFD.ChangeFlag,
    -- Cumulative sum to assign GroupAddressKey
    SUM(CASE WHEN CFD.ChangeFlag = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY CFD.STUDY_ID ORDER BY CFD.EFF_DATE, CFD.RecordID ROWS UNBOUNDED PRECEDING) AS GroupAddressKey
INTO #GroupedKeyData
FROM #ChangeFlagData CFD
ORDER BY CFD.EFF_DATE, CFD.RecordID;

-- Create index to optimize aggregation
CREATE NONCLUSTERED INDEX IX_GroupedKeyData_GroupAddressKey ON #GroupedKeyData (GroupAddressKey);
```

- **`GroupAddressKey`**: Assigns a unique group identifier to consecutive records with the same address, enabling aggregation.

### **8. Aggregate Grouped Data**

```sql
-- Drop temporary tables if they already exist
IF OBJECT_ID('tempdb..#GroupedData') IS NOT NULL DROP TABLE #GroupedData;

-- Create and populate #GroupedData with aggregated information
SELECT
    GK.STUDY_ID,
    GK.POSTAL_CODE,
    GK.STREET_FIRST_TWO_WORDS,
    MAX(GK.STREET_LINE) AS STREET_LINE,
    MIN(GK.EFF_DATE) AS EFF_DATE,
    MAX(GK.END_DATE) AS END_DATE,
    GK.GroupAddressKey
INTO #GroupedData
FROM #GroupedKeyData GK
GROUP BY GK.STUDY_ID, GK.POSTAL_CODE, GK.STREET_FIRST_TWO_WORDS, GK.GroupAddressKey;
```

- **Aggregation**: Combines records within the same `GroupAddressKey` to consolidate address changes over time.

### **9. Insert Aggregated Data into Target Table (Optimized Join)**

```sql
-- Insert the processed data into the target table
INSERT INTO DEV.FCT_HEALTH_CLIENT_ADDRESS_DATE (
    STUDY_ID,
    GroupAddressKey,
    POSTAL_CODE,
    STREET_LINE,
    EFF_DATE,
    END_DATE,
    CITY,
    LATITUDE,
    LONGITUDE
)
SELECT 
    GD.STUDY_ID,
    GD.GroupAddressKey,
    GD.POSTAL_CODE,
    GD.STREET_LINE,
    GD.EFF_DATE,
    GD.END_DATE,       
    B.[CITY], 
    B.LATITUDE, 
    B.LONGITUDE
FROM #GroupedData GD
LEFT JOIN dev.DIM_HEALTH_CLIENT_ADDRESS B
    ON GD.POSTAL_CODE = B.POSTAL_CODE 
    AND GD.STREET_LINE = B.STREET_LINE;
```

- **`LEFT JOIN`**: Associates aggregated address data with corresponding city and geographical information from `DIM_HEALTH_CLIENT_ADDRESS`.
  
### **10. Cleanup Temporary Tables for the Batch**

```sql
DROP TABLE IF EXISTS #CleanedData, #LaggedData, #ChangeFlagData, #GroupedKeyData, #GroupedData, #CurrentBatch;
```

- **Temporary Tables**: Removed after each batch to free up resources and prevent data overlap.

### **11. Update Progress and Commit Transaction**

```sql
-- Calculate Duration for the Batch
SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
SET @ProcessedCount = @ProcessedCount + (SELECT COUNT(*) FROM #CurrentBatch);
PRINT 'Processed ' + CAST(@ProcessedCount AS NVARCHAR) + ' out of ' + CAST(@TotalCount AS NVARCHAR) + ' STUDY_IDs. Current Batch Duration: ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

COMMIT TRANSACTION;
```

- **Progress Logging**: Outputs the number of `STUDY_IDs` processed and the duration of the current batch.

### **12. Error Handling**

```sql
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    PRINT 'An error occurred during batch processing.';
    PRINT ERROR_MESSAGE();

    -- Optionally, you can decide to exit the loop or continue
    BREAK;
END CATCH
```

- **`CATCH` Block**: Rolls back the transaction in case of errors and logs the error message. The `BREAK` statement exits the loop to prevent further processing in case of critical failures.

### **13. Final Completion Message**

```sql
END

PRINT 'All batches processed successfully.';
```

- **Completion Notification**: Indicates the end of the batch processing loop.

---

## **Key Enhancements and Best Practices**

1. **Batch Processing**:
    - **Scalability**: Handles large numbers of `STUDY_IDs` without overwhelming system resources.
    - **Efficiency**: Processes manageable subsets of data, reducing the risk of long-running transactions and resource contention.

2. **Enhanced Filtering with Additional Conditions**:
    - **Accurate Identification**: Incorporates `POSTAL_CODE` and `STREET_LINE` in the `LEFT JOIN` to ensure that only genuinely new records are inserted.
  
3. **Temporary Tables and Indexing**:
    - **Performance Optimization**: Creates indexes on temporary tables to speed up data retrieval and window function operations.
    - **Isolation**: Uses separate temporary tables for each batch to maintain data integrity and prevent cross-batch contamination.

4. **Transaction Management**:
    - **Data Integrity**: Ensures that each batch is processed atomically. If a batch fails, changes are rolled back to maintain consistency.
  
5. **Logging and Monitoring**:
    - **Progress Tracking**: Provides real-time feedback on the number of `STUDY_IDs` processed and the duration of each batch.
    - **Error Reporting**: Captures and logs errors to facilitate troubleshooting and ensure reliable data processing.

6. **Flexibility for Monthly Data Appends**:
    - **Seamless Integration**: The batch processing loop automatically picks up new `STUDY_IDs` appended monthly without requiring manual intervention.
    - **Adaptability**: The script can be easily adjusted to accommodate changes in data volume or processing requirements.

---

## **Final Recommendations**

1. **Adjust `@BatchSize` Appropriately**:
    - **Performance Testing**: Experiment with different batch sizes to determine the optimal number that balances performance and resource utilization for your environment.
  
2. **Automate the Batch Processing Script**:
    - **SQL Server Agent Job**: Schedule the script to run at regular intervals (e.g., nightly) using SQL Server Agent or another scheduling tool to ensure timely data processing.
  
3. **Implement Comprehensive Error Handling**:
    - **Retries**: Consider implementing retry mechanisms for transient errors to enhance robustness.
    - **Alerts**: Set up alerts to notify administrators in case of persistent failures during batch processing.
  
4. **Monitor and Maintain Indexes**:
    - **Regular Maintenance**: Periodically rebuild or reorganize indexes on the materialized table to prevent fragmentation and maintain query performance.
  
5. **Optimize Storage and Performance**:
    - **Partitioning**: For extremely large datasets, consider partitioning the materialized table based on logical keys (e.g., `EFF_DATE`) to improve query performance and manageability.
    - **Data Compression**: Explore data compression options to reduce storage footprint and potentially enhance I/O performance.
  
6. **Ensure Data Quality and Integrity**:
    - **Validation Checks**: Implement additional validation steps to verify the accuracy and completeness of the data after each batch processing.
    - **Audit Trails**: Maintain logs or audit tables to track changes and facilitate data lineage analysis.
  
7. **Documentation and Knowledge Sharing**:
    - **Process Documentation**: Clearly document the batch processing workflow, including scripts, schedules, and maintenance procedures.
    - **Training**: Ensure that team members are familiar with the processing logic and can troubleshoot or modify the scripts as needed.

---

By implementing this refactored batch processing script, you will efficiently handle multiple `STUDY_IDs`, maintain high performance even with large datasets, and seamlessly integrate monthly data appends into your materialized table. This approach ensures data integrity, scalability, and optimal resource utilization across your data processing pipeline.

Feel free to reach out if you have any further questions or need additional assistance with specific aspects of this solution!


You're correct in identifying that the combination of `(STUDY_ID, EFF_DATE)` is **not unique** in your dataset. This has significant implications for the previously suggested materialization approach, particularly regarding the **primary key constraint** and the **`WHERE NOT EXISTS`** clause in the data insertion logic.

### **Implications of Non-Unique `(STUDY_ID, EFF_DATE)`**

1. **Primary Key Constraint**:
   - **Issue**: The original solution proposed a **primary key** on `(STUDY_ID, EFF_DATE)`. If this combination is not unique, SQL Server will **reject** the table creation due to primary key violations.
   - **Impact**: Enforcing uniqueness where it doesn't exist leads to errors and prevents successful table creation and data insertion.

2. **`WHERE NOT EXISTS` Clause**:
   - **Issue**: The `WHERE NOT EXISTS` condition checks for the existence of a record with the same `(STUDY_ID, EFF_DATE)`. If multiple records can exist with the same combination, this logic may **incorrectly prevent** the insertion of legitimate duplicate records.
   - **Impact**: Potential **data loss** by not inserting all necessary records or **incorrect deduplication**.

### **Revised Materialization Strategy**

To accommodate multiple records with the same `(STUDY_ID, EFF_DATE)`, we'll implement the following adjustments:

1. **Introduce a Surrogate Primary Key**:
   - **Solution**: Add an **IDENTITY** column (e.g., `RecordID`) to serve as a **unique identifier** for each row.
   - **Benefit**: Ensures each record is uniquely identifiable without relying on non-unique business keys.

2. **Adjust Indexing Strategy**:
   - **Solution**: Remove the composite primary key on `(STUDY_ID, EFF_DATE)` and instead index these columns separately to optimize join and filter operations.
   - **Benefit**: Maintains query performance without enforcing unwanted uniqueness.

3. **Modify Data Insertion Logic**:
   - **Solution**: Remove the `WHERE NOT EXISTS` clause to allow all records, including duplicates, to be inserted.
   - **Benefit**: Preserves all data without inadvertent exclusion.

4. **Implement Comprehensive Data Refresh Procedures**:
   - **Solution**: Use `MERGE` statements or other strategies to handle inserts, updates, and deletions effectively.
   - **Benefit**: Maintains data integrity and ensures the materialized table reflects the latest state of the source view.

### **Step-by-Step Revised Materialization Guide**

Below is a comprehensive guide to materializing your view into a physical table, accommodating the non-unique `(STUDY_ID, EFF_DATE)` combination.

---

## **1. Create the Materialized Table with a Surrogate Primary Key**

```sql
-- =============================================
-- Step 1: Create Materialized Table with Surrogate Key
-- =============================================

-- Drop the table if it already exists to avoid conflicts (optional)
IF OBJECT_ID('dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE', 'U') IS NOT NULL
BEGIN
    DROP TABLE dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE;
    PRINT 'Existing table dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE dropped.';
END

-- Create the materialized table with an IDENTITY surrogate key
CREATE TABLE dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE (
    RecordID            INT IDENTITY(1,1) NOT NULL PRIMARY KEY, -- Surrogate Primary Key
    effective_year     VARCHAR(4) NOT NULL,
    effective_month    VARCHAR(2) NOT NULL,
    effective_day      VARCHAR(2) NOT NULL,
    STUDY_ID           NVARCHAR(50) NOT NULL,
    POSTAL_CODE        NVARCHAR(20) NOT NULL,
    CITY               NVARCHAR(100) NOT NULL,
    STREET_LINE        NVARCHAR(255) NOT NULL,
    LHA                NVARCHAR(50) NOT NULL,
    CHSA               NVARCHAR(50) NULL,
    LATITUDE           FLOAT NULL,
    LONGITUDE          FLOAT NULL,
    EFF_DATE           DATE NOT NULL,
    END_DATE           DATE NOT NULL
);
GO

PRINT 'Table dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE created successfully with RecordID as primary key.';
```

**Explanation:**

- **`RecordID`**: An `INT` column with `IDENTITY(1,1)` ensures each record has a unique identifier.
- **Primary Key**: Set on `RecordID` to enforce uniqueness.
- **Data Columns**: Retained as per the view's structure.

---

## **2. Index the Materialized Table Appropriately**

Creating indexes on columns frequently used in joins and filters enhances query performance.

```sql
-- =============================================
-- Step 2: Create Indexes on Materialized Table
-- =============================================

-- Non-clustered index on (STUDY_ID, EFF_DATE) to optimize queries filtering by these columns
CREATE NONCLUSTERED INDEX IX_MT_FCT_COMBINED_HEALTH_CLIENT_STUDYID_EFFDATE
ON dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE (STUDY_ID, EFF_DATE);
GO

PRINT 'Index IX_MT_FCT_COMBINED_HEALTH_CLIENT_STUDYID_EFFDATE created successfully.';

-- Non-clustered index on (POSTAL_CODE, STREET_LINE) to optimize JOIN operations
CREATE NONCLUSTERED INDEX IX_MT_FCT_COMBINED_HEALTH_CLIENT_POSTAL_STREET
ON dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE (POSTAL_CODE, STREET_LINE)
INCLUDE (CITY, LATITUDE, LONGITUDE, LHA, CHSA);
GO

PRINT 'Index IX_MT_FCT_COMBINED_HEALTH_CLIENT_POSTAL_STREET created successfully.';
```

**Explanation:**

- **`IX_MT_FCT_COMBINED_HEALTH_CLIENT_STUDYID_EFFDATE`**:
  - **Purpose**: Optimizes queries that filter on `STUDY_ID` and `EFF_DATE`, such as window functions.
  
- **`IX_MT_FCT_COMBINED_HEALTH_CLIENT_POSTAL_STREET`**:
  - **Purpose**: Enhances performance for `JOIN` operations on `POSTAL_CODE` and `STREET_LINE`.
  - **Included Columns**: `CITY`, `LATITUDE`, `LONGITUDE`, `LHA`, `CHSA` to make the index covering for relevant queries.

---

## **3. Populate the Materialized Table**

With the table and indexes in place, proceed to populate the table. Since `(STUDY_ID, EFF_DATE)` is not unique, **remove** the `WHERE NOT EXISTS` condition to allow all records to be inserted.

```sql
-- =============================================
-- Step 3: Populate Materialized Table
-- =============================================

BEGIN TRY
    BEGIN TRANSACTION;

    PRINT 'Starting data population into dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE...';

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
    SELECT
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
    FROM dev.VIEW_FCT_COMBINED_HEALTH_TABLE;

    COMMIT TRANSACTION;

    PRINT 'Data population completed successfully.';
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    PRINT 'An error occurred during data population.';
    PRINT ERROR_MESSAGE();
END CATCH
GO
```

**Explanation:**

- **Transaction Handling**: Ensures that the data population is atomic. If an error occurs, the transaction is rolled back to maintain data integrity.
- **Insertion**: Directly inserts all records from the view into the materialized table without filtering for existing records.

---

## **4. Automate Data Refresh**

To keep the materialized table up-to-date, implement a **refresh mechanism**. Below is an example using a **Stored Procedure** for a **Full Refresh**. Alternatively, you can implement an **Incremental Refresh** if your data sources support it.

### **Option A: Full Refresh (Truncate and Reload)**

**Use Case**: Suitable when the data is predominantly **append-only** and changes are infrequent or not time-sensitive.

```sql
-- =============================================
-- Step 4A: Create Stored Procedure for Full Refresh
-- =============================================

IF OBJECT_ID('dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_FullRefresh', 'P') IS NOT NULL
    DROP PROCEDURE dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_FullRefresh;
GO

CREATE PROCEDURE dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_FullRefresh
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        PRINT 'Starting full refresh of dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE...';

        -- Truncate the table to remove existing data
        TRUNCATE TABLE dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE;

        -- Reload data from the view
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
        SELECT
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
        FROM dev.VIEW_FCT_COMBINED_HEALTH_TABLE;

        COMMIT TRANSACTION;

        PRINT 'Full refresh completed successfully.';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT 'An error occurred during the full refresh.';
        PRINT ERROR_MESSAGE();
    END CATCH
END
GO

PRINT 'Stored Procedure dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_FullRefresh created successfully.';
```

**Scheduling**:

- **SQL Server Agent Job**:
  - **Step 1**: Execute the stored procedure.
    ```sql
    EXEC dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_FullRefresh;
    ```
  - **Step 2**: Schedule the job to run during off-peak hours (e.g., nightly).

**Considerations**:

- **Data Volume**: For extremely large datasets, consider implementing batch inserts or using `BULK INSERT` techniques to manage transaction log growth and reduce locking.
- **Downtime**: `TRUNCATE` is a minimally logged operation but requires exclusive access to the table. Schedule refreshes during maintenance windows to avoid conflicts.

### **Option B: Incremental Refresh**

**Use Case**: Ideal when the data is frequently updated or when only new or changed records need to be processed.

```sql
-- =============================================
-- Step 4B: Create Stored Procedure for Incremental Refresh
-- =============================================

IF OBJECT_ID('dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_IncrementalRefresh', 'P') IS NOT NULL
    DROP PROCEDURE dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_IncrementalRefresh;
GO

CREATE PROCEDURE dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_IncrementalRefresh
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        PRINT 'Starting incremental refresh of dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE...';

        -- Insert new records
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
        SELECT
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
            -- Optional: Add more conditions if needed
        WHERE mt.STUDY_ID IS NULL;

        COMMIT TRANSACTION;

        PRINT 'Incremental refresh completed successfully.';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT 'An error occurred during the incremental refresh.';
        PRINT ERROR_MESSAGE();
    END CATCH
END
GO

PRINT 'Stored Procedure dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_IncrementalRefresh created successfully.';
```

**Scheduling**:

- **SQL Server Agent Job**:
  - **Step 1**: Execute the stored procedure.
    ```sql
    EXEC dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_IncrementalRefresh;
    ```
  - **Step 2**: Schedule the job to run at appropriate intervals (e.g., hourly, daily).

**Considerations**:

- **Duplicates**: Ensure that the logic accurately identifies new records without introducing duplicates.
- **Updates and Deletions**: The provided procedure handles **inserts** only. If records can be **updated** or **deleted** in the source view, extend the procedure to handle these scenarios using `MERGE` statements.

---

## **5. Handle Updates and Deletions with `MERGE`**

To ensure the materialized table reflects the current state of the source view, handle **inserts**, **updates**, and **deletions** using the `MERGE` statement.

```sql
-- =============================================
-- Step 5: Create Stored Procedure for MERGE-Based Incremental Refresh
-- =============================================

IF OBJECT_ID('dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_MergeRefresh', 'P') IS NOT NULL
    DROP PROCEDURE dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_MergeRefresh;
GO

CREATE PROCEDURE dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_MergeRefresh
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        PRINT 'Starting MERGE-based incremental refresh of dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE...';

        MERGE dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE AS target
        USING dev.VIEW_FCT_COMBINED_HEALTH_TABLE AS source
            ON target.STUDY_ID = source.STUDY_ID
            AND target.EFF_DATE = source.EFF_DATE
            AND target.POSTAL_CODE = source.POSTAL_CODE
            AND target.STREET_LINE = source.STREET_LINE
            -- Add additional join conditions if necessary
        WHEN MATCHED THEN
            UPDATE SET
                target.effective_year = source.effective_year,
                target.effective_month = source.effective_month,
                target.effective_day = source.effective_day,
                target.CITY = source.CITY,
                target.LHA = source.LHA,
                target.CHSA = source.CHSA,
                target.LATITUDE = source.LATITUDE,
                target.LONGITUDE = source.LONGITUDE,
                target.END_DATE = source.END_DATE
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
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
            VALUES (
                source.effective_year,
                source.effective_month,
                source.effective_day,
                source.STUDY_ID,
                source.POSTAL_CODE,
                source.CITY,
                source.STREET_LINE,
                source.LHA,
                source.CHSA,
                source.LATITUDE,
                source.LONGITUDE,
                source.EFF_DATE,
                source.END_DATE
            )
        WHEN NOT MATCHED BY SOURCE THEN
            DELETE;

        COMMIT TRANSACTION;

        PRINT 'MERGE-based incremental refresh completed successfully.';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT 'An error occurred during the MERGE-based incremental refresh.';
        PRINT ERROR_MESSAGE();
    END CATCH
END
GO

PRINT 'Stored Procedure dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_MergeRefresh created successfully.';
```

**Explanation:**

- **`MERGE` Statement**: Combines `INSERT`, `UPDATE`, and `DELETE` operations in a single statement.
- **Match Conditions**: Extended to include additional columns (`POSTAL_CODE`, `STREET_LINE`) to accurately identify matching records.
- **Actions**:
  - **`WHEN MATCHED`**: Updates existing records with any changes from the source.
  - **`WHEN NOT MATCHED BY TARGET`**: Inserts new records not present in the target.
  - **`WHEN NOT MATCHED BY SOURCE`**: Deletes records from the target that no longer exist in the source.

**Scheduling**:

- **SQL Server Agent Job**:
  - **Step 1**: Execute the stored procedure.
    ```sql
    EXEC dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_MergeRefresh;
    ```
  - **Step 2**: Schedule the job based on data change frequency (e.g., daily).

**Considerations**:

- **Performance**: `MERGE` can be resource-intensive on very large datasets. Monitor performance and consider partitioning or batching if necessary.
- **Concurrency**: Ensure that the `MERGE` operation does not conflict with other operations on the table.

---

## **6. Adjust Data Refresh Strategy in Line with Non-Unique Keys**

Given that `(STUDY_ID, EFF_DATE)` is not unique, ensure that your **querying and processing logic** downstream can handle multiple records per combination.

### **Impact on Existing Processing Steps**

Your original processing script utilizes window functions partitioned by `STUDY_ID` and ordered by `EFF_DATE`. With multiple records per `(STUDY_ID, EFF_DATE)`, ensure that:

1. **Partitioning Logic**: It correctly partitions data considering all relevant fields.
2. **Window Functions**: They operate as intended, possibly considering additional columns to maintain logical consistency.

### **Example Adjustment in Window Functions**

If multiple records per `(STUDY_ID, EFF_DATE)` exist, you may need to introduce additional partitioning criteria or unique identifiers.

```sql
-- Example: Adjusting Window Functions to Handle Multiple Records

SELECT
    CD.STUDY_ID,
    CD.POSTAL_CODE,
    CD.STREET_LINE,
    CD.STREET_FIRST_TWO_WORDS,
    CD.EFF_DATE,
    CD.END_DATE,
    LAG(CD.POSTAL_CODE) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE, CD.RecordID) AS Prev_POSTAL_CODE,
    LAG(CD.STREET_FIRST_TWO_WORDS) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE, CD.RecordID) AS Prev_STREET_FIRST_TWO_WORDS
FROM dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE CD
ORDER BY CD.EFF_DATE, CD.RecordID;
```

**Explanation:**

- **Ordering**: Adds `RecordID` to the `ORDER BY` clause to ensure consistent ordering when multiple records share the same `EFF_DATE`.
- **Partitioning**: Maintains partitioning by `STUDY_ID` only, as `EFF_DATE` is already part of the ordering.

**Note**: Adjust the window functions based on your specific analytical requirements.

---

## **7. Comprehensive Refactored Materialization Script**

Below is an updated and comprehensive script that incorporates all the necessary adjustments to handle non-unique `(STUDY_ID, EFF_DATE)` combinations.

```sql
-- =============================================
-- Step 1: Create Materialized Table with Surrogate Key
-- =============================================

-- Drop the table if it exists (optional)
IF OBJECT_ID('dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE', 'U') IS NOT NULL
BEGIN
    DROP TABLE dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE;
    PRINT 'Existing table dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE dropped.';
END

-- Create the materialized table with an IDENTITY surrogate key
CREATE TABLE dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE (
    RecordID            INT IDENTITY(1,1) NOT NULL PRIMARY KEY, -- Surrogate Primary Key
    effective_year     VARCHAR(4) NOT NULL,
    effective_month    VARCHAR(2) NOT NULL,
    effective_day      VARCHAR(2) NOT NULL,
    STUDY_ID           NVARCHAR(50) NOT NULL,
    POSTAL_CODE        NVARCHAR(20) NOT NULL,
    CITY               NVARCHAR(100) NOT NULL,
    STREET_LINE        NVARCHAR(255) NOT NULL,
    LHA                NVARCHAR(50) NOT NULL,
    CHSA               NVARCHAR(50) NULL,
    LATITUDE           FLOAT NULL,
    LONGITUDE          FLOAT NULL,
    EFF_DATE           DATE NOT NULL,
    END_DATE           DATE NOT NULL
);
GO

PRINT 'Table dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE created successfully with RecordID as primary key.';

-- =============================================
-- Step 2: Create Indexes on Materialized Table
-- =============================================

-- Non-clustered index on (STUDY_ID, EFF_DATE) to optimize queries filtering by these columns
CREATE NONCLUSTERED INDEX IX_MT_FCT_COMBINED_HEALTH_CLIENT_STUDYID_EFFDATE
ON dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE (STUDY_ID, EFF_DATE);
GO

PRINT 'Index IX_MT_FCT_COMBINED_HEALTH_CLIENT_STUDYID_EFFDATE created successfully.';

-- Non-clustered index on (POSTAL_CODE, STREET_LINE) to optimize JOIN operations
CREATE NONCLUSTERED INDEX IX_MT_FCT_COMBINED_HEALTH_CLIENT_POSTAL_STREET
ON dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE (POSTAL_CODE, STREET_LINE)
INCLUDE (CITY, LATITUDE, LONGITUDE, LHA, CHSA);
GO

PRINT 'Index IX_MT_FCT_COMBINED_HEALTH_CLIENT_POSTAL_STREET created successfully.';

-- =============================================
-- Step 3: Populate Materialized Table
-- =============================================

BEGIN TRY
    BEGIN TRANSACTION;

    PRINT 'Starting data population into dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE...';

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
    SELECT
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
    FROM dev.VIEW_FCT_COMBINED_HEALTH_TABLE;

    COMMIT TRANSACTION;

    PRINT 'Data population completed successfully.';
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    PRINT 'An error occurred during data population.';
    PRINT ERROR_MESSAGE();
END CATCH
GO

-- =============================================
-- Step 4: Create Stored Procedure for MERGE-Based Incremental Refresh
-- =============================================

IF OBJECT_ID('dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_MergeRefresh', 'P') IS NOT NULL
    DROP PROCEDURE dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_MergeRefresh;
GO

CREATE PROCEDURE dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_MergeRefresh
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        PRINT 'Starting MERGE-based incremental refresh of dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE...';

        MERGE dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE AS target
        USING dev.VIEW_FCT_COMBINED_HEALTH_TABLE AS source
            ON target.STUDY_ID = source.STUDY_ID
            AND target.EFF_DATE = source.EFF_DATE
            AND target.POSTAL_CODE = source.POSTAL_CODE
            AND target.STREET_LINE = source.STREET_LINE
        WHEN MATCHED THEN
            UPDATE SET
                target.effective_year = source.effective_year,
                target.effective_month = source.effective_month,
                target.effective_day = source.effective_day,
                target.CITY = source.CITY,
                target.LHA = source.LHA,
                target.CHSA = source.CHSA,
                target.LATITUDE = source.LATITUDE,
                target.LONGITUDE = source.LONGITUDE,
                target.END_DATE = source.END_DATE
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
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
            VALUES (
                source.effective_year,
                source.effective_month,
                source.effective_day,
                source.STUDY_ID,
                source.POSTAL_CODE,
                source.CITY,
                source.STREET_LINE,
                source.LHA,
                source.CHSA,
                source.LATITUDE,
                source.LONGITUDE,
                source.EFF_DATE,
                source.END_DATE
            )
        WHEN NOT MATCHED BY SOURCE THEN
            DELETE;

        COMMIT TRANSACTION;

        PRINT 'MERGE-based incremental refresh completed successfully.';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT 'An error occurred during the MERGE-based incremental refresh.';
        PRINT ERROR_MESSAGE();
    END CATCH
END
GO

PRINT 'Stored Procedure dev.SP_Refresh_MT_FCT_COMBINED_HEALTH_CLIENT_TABLE_MergeRefresh created successfully.';
```

**Explanation:**

1. **Step 1: Create Materialized Table**
   - **Surrogate Key**: `RecordID` ensures each record is uniquely identifiable.
   - **Primary Key**: Enforced on `RecordID` instead of `(STUDY_ID, EFF_DATE)`.
   
2. **Step 2: Create Indexes**
   - **`IX_MT_FCT_COMBINED_HEALTH_CLIENT_STUDYID_EFFDATE`**: Enhances performance for filtering and window functions based on `STUDY_ID` and `EFF_DATE`.
   - **`IX_MT_FCT_COMBINED_HEALTH_CLIENT_POSTAL_STREET`**: Optimizes `JOIN` operations with `DIM_HEALTH_CLIENT_ADDRESS`.
   
3. **Step 3: Populate Materialized Table**
   - **Insertion Logic**: Inserts all records from the view without filtering, ensuring no data is excluded.
   
4. **Step 4: Create Stored Procedure for `MERGE`-Based Incremental Refresh**
   - **`MERGE` Statement**: Handles `INSERT`, `UPDATE`, and `DELETE` operations in one atomic transaction.
   - **Match Conditions**: Extended to include `POSTAL_CODE` and `STREET_LINE` to accurately identify matching records.
   - **Automation**: Schedule this stored procedure via SQL Server Agent to run at desired intervals (e.g., nightly).

---

## **8. Update Your Processing Script to Use the Materialized Table**

With the materialized table in place, update your processing scripts to reference `dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE` instead of the view. Ensure that the script accounts for the `RecordID` and possible multiple records per `(STUDY_ID, EFF_DATE)`.

### **Example Adjusted Processing Script Snippet**

```sql
-- Adjusted Step 1: Clean and Prepare Data

-- Use the materialized table instead of the view
SELECT
    STUDY_ID,
    POSTAL_CODE,
    STREET_LINE,
    -- Extract the first two words from STREET_LINE
    CASE 
        WHEN STREET_LINE IS NOT NULL THEN
            CASE
                WHEN CHARINDEX(' ', STREET_LINE) > 0 THEN
                    LEFT(
                        STREET_LINE,
                        CHARINDEX(' ', STREET_LINE + ' ', CHARINDEX(' ', STREET_LINE + ' ') + 1) - 1
                    )
                ELSE STREET_LINE
            END
        ELSE STREET_LINE
    END AS STREET_FIRST_TWO_WORDS,
    CAST(EFF_DATE AS DATE) AS EFF_DATE,
    CAST(END_DATE AS DATE) AS END_DATE
INTO #CleanedData
FROM dev.MT_FCT_COMBINED_HEALTH_CLIENT_TABLE
WHERE STUDY_ID = '001A9042FA9A48CC00E28DD031448F25B9CE08AA676282D6'; -- Replace with your STUDY_ID

-- Proceed with the rest of your processing steps as previously defined
```

**Considerations:**

- **Handling Multiple Records**: Ensure that downstream steps correctly handle multiple records per `(STUDY_ID, EFF_DATE)`. This may involve adjusting window function partitioning or aggregation logic.
  
  **Example Adjustment:**

  ```sql
  -- Compute Previous Values with additional ordering to handle multiple records
  SELECT
      CD.STUDY_ID,
      CD.POSTAL_CODE,
      CD.STREET_LINE,
      CD.STREET_FIRST_TWO_WORDS,
      CD.EFF_DATE,
      CD.END_DATE,
      LAG(CD.POSTAL_CODE) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE, CD.RecordID) AS Prev_POSTAL_CODE,
      LAG(CD.STREET_FIRST_TWO_WORDS) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE, CD.RecordID) AS Prev_STREET_FIRST_TWO_WORDS
  INTO #LaggedData
  FROM #CleanedData CD
  ORDER BY CD.EFF_DATE, CD.RecordID;
  ```

- **Indexes Utilization**: Ensure that your temporary tables (`#CleanedData`, `#LaggedData`, etc.) have appropriate indexes to optimize window functions and joins.

---

## **9. Additional Best Practices**

1. **Error Handling and Logging**:
   - Implement robust error handling in your stored procedures and processing scripts.
   - Log significant events and errors to facilitate troubleshooting and auditing.

2. **Performance Monitoring**:
   - Continuously monitor the performance of your materialized table and adjust indexing strategies as necessary.
   - Use **Azure Data Studio's Execution Plans** and **Dynamic Management Views (DMVs)** to identify and resolve performance issues.

3. **Backup and Recovery**:
   - Include the materialized table in your database backup strategy to prevent data loss.
   - Test recovery procedures to ensure data can be restored accurately.

4. **Security and Permissions**:
   - Ensure that only authorized users have access to the materialized table.
   - Implement role-based access controls to maintain data security.

5. **Documentation**:
   - Document the materialization process, including table structures, indexing strategies, and refresh schedules.
   - Maintain clear documentation to aid future maintenance and onboarding.

---

## **10. Final Thoughts**

By introducing a surrogate primary key and adjusting the data insertion and indexing strategies, you effectively accommodate multiple records per `(STUDY_ID, EFF_DATE)` combination. This ensures data integrity, prevents insertion errors, and maintains optimal query performance.

**Next Steps:**

1. **Execute the Revised Materialization Script**:
   - Run the provided scripts in a development or staging environment first to validate functionality and performance.

2. **Validate Data Integrity**:
   - Ensure that all records from the view are accurately reflected in the materialized table.
   - Verify that duplicate records are handled as intended.

3. **Implement and Schedule Data Refresh Procedures**:
   - Choose between **Full Refresh** and **Incremental Refresh** based on your data update patterns.
   - Schedule the appropriate stored procedures via SQL Server Agent or Azure Automation.

4. **Update Downstream Processes**:
   - Modify your existing processing scripts to reference the materialized table.
   - Test the entire data pipeline to ensure seamless integration and performance improvements.

5. **Monitor and Optimize**:
   - After deployment, monitor the performance and make necessary adjustments to indexes, partitioning, or refresh frequencies.

Feel free to reach out if you need further assistance with specific aspects of the materialization process or encounter any challenges during implementation.




To effectively measure the execution time of each step in your refactored SQL query using **Azure Data Studio (ADS)**, you can incorporate **T-SQL timing mechanisms** directly into your script. This approach allows you to capture and log the time taken by each individual operation, such as creating temporary tables, performing updates, aggregations, and inserts.

Here’s a comprehensive guide on how to implement this:

## **1. Using T-SQL Variables and `PRINT` Statements**

By leveraging T-SQL's `GETDATE()` or `SYSDATETIME()` functions, you can capture timestamps before and after each step, calculate the duration, and output the results using `PRINT` statements.

### **Step-by-Step Implementation**

Below is a refactored version of your query with timing measurements added for each significant step:

```sql
-- =============================================
-- Step 0: Initialize Timing Variables
-- =============================================

DECLARE @StartTime DATETIME2, @EndTime DATETIME2, @DurationSeconds FLOAT;

-- =============================================
-- Step 1: Clean and Prepare Data
-- =============================================

SET @StartTime = SYSDATETIME();

-- Drop temporary tables if they already exist
IF OBJECT_ID('tempdb..#CleanedData') IS NOT NULL DROP TABLE #CleanedData;

-- Create and populate #CleanedData with necessary transformations
SELECT
    STUDY_ID,
    POSTAL_CODE,
    STREET_LINE,
    -- Extract the first two words from STREET_LINE
    CASE 
        WHEN STREET_LINE IS NOT NULL THEN
            CASE
                WHEN CHARINDEX(' ', STREET_LINE) > 0 THEN
                    LEFT(
                        STREET_LINE,
                        CHARINDEX(' ', STREET_LINE + ' ', CHARINDEX(' ', STREET_LINE + ' ') + 1) - 1
                    )
                ELSE STREET_LINE
            END
        ELSE STREET_LINE
    END AS STREET_FIRST_TWO_WORDS,
    CAST(EFF_DATE AS DATE) AS EFF_DATE,
    CAST(END_DATE AS DATE) AS END_DATE
INTO #CleanedData
FROM [HealthFiles_test].[dev].[FCT_COMBINED_HEALTH_CLIENT_TABLE]
WHERE STUDY_ID = '001A9042FA9A48CC00E28DD031448F25B9CE08AA676282D6'; -- Replace with your STUDY_ID

-- Create indexes to optimize subsequent operations
CREATE NONCLUSTERED INDEX IX_CleanedData_EFF_DATE ON #CleanedData (STUDY_ID, EFF_DATE);

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 1: Clean and Prepare Data took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 2: Compute Previous Values Using Window Functions
-- =============================================

SET @StartTime = SYSDATETIME();

-- Create and populate #LaggedData with previous postal code and street words
SELECT
    CD.STUDY_ID,
    CD.POSTAL_CODE,
    CD.STREET_LINE,
    CD.STREET_FIRST_TWO_WORDS,
    CD.EFF_DATE,
    CD.END_DATE,
    LAG(CD.POSTAL_CODE) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE) AS Prev_POSTAL_CODE,
    LAG(CD.STREET_FIRST_TWO_WORDS) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE) AS Prev_STREET_FIRST_TWO_WORDS
INTO #LaggedData
FROM #CleanedData CD
ORDER BY CD.EFF_DATE;

-- Create index to optimize window functions
CREATE NONCLUSTERED INDEX IX_LaggedData_EFF_DATE ON #LaggedData (EFF_DATE);

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 2: Compute Previous Values Using Window Functions took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 3: Flag Changes in Address
-- =============================================

SET @StartTime = SYSDATETIME();

-- Create and populate #ChangeFlagData with ChangeFlag
SELECT
    LD.STUDY_ID,
    LD.POSTAL_CODE,
    LD.STREET_LINE,
    LD.STREET_FIRST_TWO_WORDS,
    LD.EFF_DATE,
    LD.END_DATE,
    LD.Prev_POSTAL_CODE,
    LD.Prev_STREET_FIRST_TWO_WORDS,
    CASE 
        WHEN LD.Prev_POSTAL_CODE IS NULL 
             OR LD.Prev_STREET_FIRST_TWO_WORDS IS NULL 
             OR LD.Prev_POSTAL_CODE != LD.POSTAL_CODE 
             OR LD.Prev_STREET_FIRST_TWO_WORDS != LD.STREET_FIRST_TWO_WORDS 
        THEN 1 
        ELSE 0 
    END AS ChangeFlag
INTO #ChangeFlagData
FROM #LaggedData LD
ORDER BY LD.EFF_DATE;

-- Create index to optimize subsequent operations
CREATE NONCLUSTERED INDEX IX_ChangeFlagData_EFF_DATE ON #ChangeFlagData (EFF_DATE);

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 3: Flag Changes in Address took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 4: Assign GroupAddressKey Using Cumulative Sum
-- =============================================

SET @StartTime = SYSDATETIME();

-- Create and populate #GroupedKeyData with GroupAddressKey
SELECT
    CFD.STUDY_ID,
    CFD.POSTAL_CODE,
    CFD.STREET_LINE,
    CFD.STREET_FIRST_TWO_WORDS,
    CFD.EFF_DATE,
    CFD.END_DATE,
    CFD.Prev_POSTAL_CODE,
    CFD.Prev_STREET_FIRST_TWO_WORDS,
    CFD.ChangeFlag,
    -- Cumulative sum to assign GroupAddressKey
    SUM(CASE WHEN CFD.ChangeFlag = 1 THEN 1 ELSE 0 END) OVER (ORDER BY CFD.EFF_DATE ROWS UNBOUNDED PRECEDING) AS GroupAddressKey
INTO #GroupedKeyData
FROM #ChangeFlagData CFD
ORDER BY CFD.EFF_DATE;

-- Create index to optimize aggregation
CREATE NONCLUSTERED INDEX IX_GroupedKeyData_GroupAddressKey ON #GroupedKeyData (GroupAddressKey);

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 4: Assign GroupAddressKey Using Cumulative Sum took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 5: Aggregate Grouped Data
-- =============================================

SET @StartTime = SYSDATETIME();

-- Create and populate #GroupedData with aggregated information
SELECT
    GK.STUDY_ID,
    GK.POSTAL_CODE,
    GK.STREET_FIRST_TWO_WORDS,
    MAX(GK.STREET_LINE) AS STREET_LINE,
    MIN(GK.EFF_DATE) AS EFF_DATE,
    MAX(GK.END_DATE) AS END_DATE,
    GK.GroupAddressKey
INTO #GroupedData
FROM #GroupedKeyData GK
GROUP BY GK.STUDY_ID, GK.POSTAL_CODE, GK.STREET_FIRST_TWO_WORDS, GK.GroupAddressKey;

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 5: Aggregate Grouped Data took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 6: Insert Aggregated Data into Target Table
-- =============================================

SET @StartTime = SYSDATETIME();

-- Insert the processed data into the target table
INSERT INTO DEV.FCT_HEALTH_CLIENT_ADDRESS_DATE (
    STUDY_ID,
    GroupAddressKey,
    POSTAL_CODE,
    STREET_LINE,
    EFF_DATE,
    END_DATE,
    CITY,
    LATITUDE,
    LONGITUDE
)
SELECT 
    GD.STUDY_ID,
    GD.GroupAddressKey,
    GD.POSTAL_CODE,
    GD.STREET_LINE,
    GD.EFF_DATE,
    GD.END_DATE,       
    B.[CITY], 
    B.LATITUDE, 
    B.LONGITUDE
FROM #GroupedData GD
LEFT JOIN dev.DIM_HEALTH_CLIENT_ADDRESS B
    ON GD.POSTAL_CODE = B.POSTAL_CODE 
    AND GD.STREET_LINE = B.STREET_LINE;

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 6: Insert Aggregated Data into Target Table took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 7: Cleanup Temporary Tables
-- =============================================

SET @StartTime = SYSDATETIME();

DROP TABLE IF EXISTS #CleanedData, #LaggedData, #ChangeFlagData, #GroupedKeyData, #GroupedData;

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 7: Cleanup Temporary Tables took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';
```

### **Explanation of the Refactored Script**

1. **Initialization**:
    - **Variables**: `@StartTime`, `@EndTime`, and `@DurationSeconds` are declared to capture the start and end times of each step and calculate the duration in seconds.

2. **Step 1: Clean and Prepare Data**:
    - **Data Selection and Transformation**: Extracts relevant columns and computes `STREET_FIRST_TWO_WORDS` by extracting the first two words from `STREET_LINE`.
    - **Temporary Table**: Stores the cleaned data in `#CleanedData`.
    - **Indexing**: Creates a non-clustered index on `(STUDY_ID, EFF_DATE)` to optimize window functions and ordering.
    - **Timing**: Captures and prints the time taken for this step.

3. **Step 2: Compute Previous Values Using Window Functions**:
    - **Window Functions**: Uses `LAG` to fetch previous `POSTAL_CODE` and `STREET_FIRST_TWO_WORDS`.
    - **Temporary Table**: Stores the lagged data in `#LaggedData`.
    - **Indexing**: Creates a non-clustered index on `(EFF_DATE)` to optimize the `ORDER BY` clause used in window functions.
    - **Timing**: Captures and prints the time taken for this step.

4. **Step 3: Flag Changes in Address**:
    - **Change Flag Calculation**: Determines if there's a change in `POSTAL_CODE` or `STREET_FIRST_TWO_WORDS` compared to the previous record.
    - **Temporary Table**: Stores the flagged data in `#ChangeFlagData`.
    - **Indexing**: Creates a non-clustered index on `(EFF_DATE)` to facilitate efficient querying in subsequent steps.
    - **Timing**: Captures and prints the time taken for this step.

5. **Step 4: Assign GroupAddressKey Using Cumulative Sum**:
    - **Cumulative Sum**: Assigns a `GroupAddressKey` by cumulatively summing the `ChangeFlag`. This groups consecutive records with the same address.
    - **Temporary Table**: Stores the grouped key data in `#GroupedKeyData`.
    - **Indexing**: Creates a non-clustered index on `(GroupAddressKey)` to optimize the aggregation process.
    - **Timing**: Captures and prints the time taken for this step.

6. **Step 5: Aggregate Grouped Data**:
    - **Aggregation**: For each group identified by `GroupAddressKey`, aggregates the data to obtain `STREET_LINE`, `EFF_DATE`, and `END_DATE`.
    - **Temporary Table**: Stores the aggregated data in `#GroupedData`.
    - **Timing**: Captures and prints the time taken for this step.

7. **Step 6: Insert Aggregated Data into Target Table**:
    - **Joining with Dimension Table**: Performs a `LEFT JOIN` with `DIM_HEALTH_CLIENT_ADDRESS` to fetch additional details like `CITY`, `LATITUDE`, and `LONGITUDE`.
    - **Insertion**: Inserts the final processed data into `DEV.FCT_HEALTH_CLIENT_ADDRESS_DATE`.
    - **Timing**: Captures and prints the time taken for this step.

8. **Step 7: Cleanup Temporary Tables**:
    - **Dropping Tables**: Drops all temporary tables to free up `tempdb` space immediately after processing.
    - **Timing**: Captures and prints the time taken for this step.

### **2. Executing the Script in Azure Data Studio**

When you run this script in Azure Data Studio:

- **Messages Pane**: The `PRINT` statements will output the duration of each step in the **Messages** pane.
  
  Example Output:
  ```
  Step 1: Clean and Prepare Data took 30 seconds.
  Step 2: Compute Previous Values Using Window Functions took 45 seconds.
  Step 3: Flag Changes in Address took 20 seconds.
  Step 4: Assign GroupAddressKey Using Cumulative Sum took 15 seconds.
  Step 5: Aggregate Grouped Data took 10 seconds.
  Step 6: Insert Aggregated Data into Target Table took 25 seconds.
  Step 7: Cleanup Temporary Tables took 5 seconds.
  ```

### **3. Advanced Timing with `SQLCMD` Mode (Optional)**

Azure Data Studio supports `SQLCMD` mode, which allows for more advanced scripting capabilities, including variable substitution and scripting commands. However, for the purpose of measuring execution time of each step, the method using T-SQL variables and `PRINT` statements is sufficient and straightforward.

### **4. Utilizing `SET STATISTICS TIME` and `SET STATISTICS IO`**

Additionally, you can use `SET STATISTICS TIME ON` and `SET STATISTICS IO ON` to gather more detailed information about the query execution, such as CPU time, elapsed time, and I/O statistics.

```sql
SET STATISTICS TIME ON;
SET STATISTICS IO ON;

-- Your SQL Query Here

SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
```

- **Output**: The statistics will appear in the **Messages** pane, providing insights into each operation's resource consumption.

**Note**: `SET STATISTICS TIME` and `SET STATISTICS IO` provide cumulative statistics for the entire batch or query, not for individual steps. To get per-step statistics, the T-SQL timing approach with variables and `PRINT` statements is more effective.

### **5. Monitoring Execution with Query Execution Plans**

Azure Data Studio offers **Execution Plans**, which visually represent the data retrieval methods chosen by the SQL Server query optimizer.

- **How to View**:
  - Before executing your query, click on the "Include Actual Execution Plan" button (it looks like a query plan icon) in the toolbar.
  - Execute your script.
  - The execution plan will appear in a separate tab, allowing you to analyze the performance characteristics of each step.

**Benefits**:
- **Identify Bottlenecks**: Spot inefficient operations like table scans, missing indexes, or expensive joins.
- **Optimization Opportunities**: Use the insights to further refine your query and indexing strategy.

### **6. Example Refactored Script with Timing**

Here’s the complete refactored script incorporating all the timing mechanisms discussed:

```sql
-- =============================================
-- Step 0: Initialize Timing Variables
-- =============================================

DECLARE @StartTime DATETIME2, @EndTime DATETIME2, @DurationSeconds FLOAT;

-- =============================================
-- Step 1: Clean and Prepare Data
-- =============================================

SET @StartTime = SYSDATETIME();

-- Drop temporary tables if they already exist
IF OBJECT_ID('tempdb..#CleanedData') IS NOT NULL DROP TABLE #CleanedData;

-- Create and populate #CleanedData with necessary transformations
SELECT
    STUDY_ID,
    POSTAL_CODE,
    STREET_LINE,
    -- Extract the first two words from STREET_LINE
    CASE 
        WHEN STREET_LINE IS NOT NULL THEN
            CASE
                WHEN CHARINDEX(' ', STREET_LINE) > 0 THEN
                    LEFT(
                        STREET_LINE,
                        CHARINDEX(' ', STREET_LINE + ' ', CHARINDEX(' ', STREET_LINE + ' ') + 1) - 1
                    )
                ELSE STREET_LINE
            END
        ELSE STREET_LINE
    END AS STREET_FIRST_TWO_WORDS,
    CAST(EFF_DATE AS DATE) AS EFF_DATE,
    CAST(END_DATE AS DATE) AS END_DATE
INTO #CleanedData
FROM [HealthFiles_test].[dev].[FCT_COMBINED_HEALTH_CLIENT_TABLE]
WHERE STUDY_ID = '001A9042FA9A48CC00E28DD031448F25B9CE08AA676282D6'; -- Replace with your STUDY_ID

-- Create indexes to optimize subsequent operations
CREATE NONCLUSTERED INDEX IX_CleanedData_EFF_DATE ON #CleanedData (STUDY_ID, EFF_DATE);

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 1: Clean and Prepare Data took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 2: Compute Previous Values Using Window Functions
-- =============================================

SET @StartTime = SYSDATETIME();

-- Create and populate #LaggedData with previous postal code and street words
SELECT
    CD.STUDY_ID,
    CD.POSTAL_CODE,
    CD.STREET_LINE,
    CD.STREET_FIRST_TWO_WORDS,
    CD.EFF_DATE,
    CD.END_DATE,
    LAG(CD.POSTAL_CODE) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE) AS Prev_POSTAL_CODE,
    LAG(CD.STREET_FIRST_TWO_WORDS) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE) AS Prev_STREET_FIRST_TWO_WORDS
INTO #LaggedData
FROM #CleanedData CD
ORDER BY CD.EFF_DATE;

-- Create index to optimize window functions
CREATE NONCLUSTERED INDEX IX_LaggedData_EFF_DATE ON #LaggedData (EFF_DATE);

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 2: Compute Previous Values Using Window Functions took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 3: Flag Changes in Address
-- =============================================

SET @StartTime = SYSDATETIME();

-- Create and populate #ChangeFlagData with ChangeFlag
SELECT
    LD.STUDY_ID,
    LD.POSTAL_CODE,
    LD.STREET_LINE,
    LD.STREET_FIRST_TWO_WORDS,
    LD.EFF_DATE,
    LD.END_DATE,
    LD.Prev_POSTAL_CODE,
    LD.Prev_STREET_FIRST_TWO_WORDS,
    CASE 
        WHEN LD.Prev_POSTAL_CODE IS NULL 
             OR LD.Prev_STREET_FIRST_TWO_WORDS IS NULL 
             OR LD.Prev_POSTAL_CODE != LD.POSTAL_CODE 
             OR LD.Prev_STREET_FIRST_TWO_WORDS != LD.STREET_FIRST_TWO_WORDS 
        THEN 1 
        ELSE 0 
    END AS ChangeFlag
INTO #ChangeFlagData
FROM #LaggedData LD
ORDER BY LD.EFF_DATE;

-- Create index to optimize subsequent operations
CREATE NONCLUSTERED INDEX IX_ChangeFlagData_EFF_DATE ON #ChangeFlagData (EFF_DATE);

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 3: Flag Changes in Address took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 4: Assign GroupAddressKey Using Cumulative Sum
-- =============================================

SET @StartTime = SYSDATETIME();

-- Create and populate #GroupedKeyData with GroupAddressKey
SELECT
    CFD.STUDY_ID,
    CFD.POSTAL_CODE,
    CFD.STREET_LINE,
    CFD.STREET_FIRST_TWO_WORDS,
    CFD.EFF_DATE,
    CFD.END_DATE,
    CFD.Prev_POSTAL_CODE,
    CFD.Prev_STREET_FIRST_TWO_WORDS,
    CFD.ChangeFlag,
    -- Cumulative sum to assign GroupAddressKey
    SUM(CASE WHEN CFD.ChangeFlag = 1 THEN 1 ELSE 0 END) OVER (ORDER BY CFD.EFF_DATE ROWS UNBOUNDED PRECEDING) AS GroupAddressKey
INTO #GroupedKeyData
FROM #ChangeFlagData CFD
ORDER BY CFD.EFF_DATE;

-- Create index to optimize aggregation
CREATE NONCLUSTERED INDEX IX_GroupedKeyData_GroupAddressKey ON #GroupedKeyData (GroupAddressKey);

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 4: Assign GroupAddressKey Using Cumulative Sum took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 5: Aggregate Grouped Data
-- =============================================

SET @StartTime = SYSDATETIME();

-- Create and populate #GroupedData with aggregated information
SELECT
    GK.STUDY_ID,
    GK.POSTAL_CODE,
    GK.STREET_FIRST_TWO_WORDS,
    MAX(GK.STREET_LINE) AS STREET_LINE,
    MIN(GK.EFF_DATE) AS EFF_DATE,
    MAX(GK.END_DATE) AS END_DATE,
    GK.GroupAddressKey
INTO #GroupedData
FROM #GroupedKeyData GK
GROUP BY GK.STUDY_ID, GK.POSTAL_CODE, GK.STREET_FIRST_TWO_WORDS, GK.GroupAddressKey;

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 5: Aggregate Grouped Data took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 6: Insert Aggregated Data into Target Table
-- =============================================

SET @StartTime = SYSDATETIME();

-- Insert the processed data into the target table
INSERT INTO DEV.FCT_HEALTH_CLIENT_ADDRESS_DATE (
    STUDY_ID,
    GroupAddressKey,
    POSTAL_CODE,
    STREET_LINE,
    EFF_DATE,
    END_DATE,
    CITY,
    LATITUDE,
    LONGITUDE
)
SELECT 
    GD.STUDY_ID,
    GD.GroupAddressKey,
    GD.POSTAL_CODE,
    GD.STREET_LINE,
    GD.EFF_DATE,
    GD.END_DATE,       
    B.[CITY], 
    B.LATITUDE, 
    B.LONGITUDE
FROM #GroupedData GD
LEFT JOIN dev.DIM_HEALTH_CLIENT_ADDRESS B
    ON GD.POSTAL_CODE = B.POSTAL_CODE 
    AND GD.STREET_LINE = B.STREET_LINE;

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 6: Insert Aggregated Data into Target Table took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';

-- =============================================
-- Step 7: Cleanup Temporary Tables
-- =============================================

SET @StartTime = SYSDATETIME();

DROP TABLE IF EXISTS #CleanedData, #LaggedData, #ChangeFlagData, #GroupedKeyData, #GroupedData;

SET @EndTime = SYSDATETIME();
SET @DurationSeconds = DATEDIFF(SECOND, @StartTime, @EndTime);
PRINT 'Step 7: Cleanup Temporary Tables took ' + CAST(@DurationSeconds AS NVARCHAR) + ' seconds.';
```

### **Key Points in the Refactored Script**

1. **Timing Variables**:
    - `@StartTime`: Captures the timestamp at the beginning of a step.
    - `@EndTime`: Captures the timestamp at the end of a step.
    - `@DurationSeconds`: Calculates the duration in seconds between `@StartTime` and `@EndTime`.

2. **`PRINT` Statements**:
    - After each step, a `PRINT` statement outputs the time taken for that specific step. These messages appear in the **Messages** pane in Azure Data Studio.

3. **Temporary Tables and Indexes**:
    - Each temporary table (`#CleanedData`, `#LaggedData`, etc.) is created, populated, and indexed to optimize performance.
    - Dropping temporary tables after their usage ensures efficient `tempdb` utilization.

4. **Step Identification**:
    - Clear comments and step demarcations help in understanding the flow and isolating performance metrics for each operation.

### **6. Running and Interpreting the Script in Azure Data Studio**

1. **Open Azure Data Studio**:
    - Launch ADS and connect to your SQL Server instance.

2. **Create a New Query**:
    - Open a new query editor window.

3. **Paste the Refactored Script**:
    - Insert the refactored script with timing measurements into the editor.

4. **Execute the Query**:
    - Run the script by clicking the **Run** button or pressing `F5`.

5. **View the Results**:
    - Switch to the **Messages** pane to see the `PRINT` outputs indicating the duration of each step.
    
    Example Output:
    ```
    Step 1: Clean and Prepare Data took 30 seconds.
    Step 2: Compute Previous Values Using Window Functions took 45 seconds.
    Step 3: Flag Changes in Address took 20 seconds.
    Step 4: Assign GroupAddressKey Using Cumulative Sum took 15 seconds.
    Step 5: Aggregate Grouped Data took 10 seconds.
    Step 6: Insert Aggregated Data into Target Table took 25 seconds.
    Step 7: Cleanup Temporary Tables took 5 seconds.
    ```

6. **Analyze the Timing Information**:
    - **Identify Bottlenecks**: Steps with unusually high durations indicate potential performance issues.
    - **Focus Optimization Efforts**: Prioritize optimizing steps that consume the most time.

### **7. Additional Tools and Techniques in Azure Data Studio**

While the above method provides a straightforward way to measure step durations, Azure Data Studio offers additional tools and extensions that can aid in performance monitoring and optimization:

#### **A. Execution Plans**

- **View Execution Plans**:
    - Click on the "Include Actual Execution Plan" button (icon resembling a query plan) before executing your query.
    - After execution, the execution plan appears in a separate tab, visualizing the query's execution steps and highlighting potential inefficiencies.

- **Analyze Execution Plans**:
    - Look for operations with high cost percentages.
    - Identify missing indexes or operations causing table scans.

#### **B. Query Performance Insights Extension**

- **Install the Extension**:
    - Go to the **Extensions** view in ADS (`Ctrl+Shift+X`).
    - Search for **"Query Performance Insights"** or similar extensions that provide advanced performance metrics.

- **Use the Extension**:
    - These extensions can offer real-time monitoring, historical performance data, and detailed analytics to help optimize your queries further.

#### **C. Activity Monitor via DMVs**

- **Use Dynamic Management Views (DMVs)**:
    - Execute queries against DMVs to monitor server performance and resource utilization during query execution.
    
    Example Query:
    ```sql
    SELECT 
        session_id,
        request_id,
        start_time,
        status,
        command,
        total_elapsed_time,
        cpu_time,
        reads,
        writes
    FROM sys.dm_exec_requests
    WHERE session_id > 50; -- Exclude system sessions
    ```

- **Interpret DMV Data**:
    - **`total_elapsed_time`**: Time in milliseconds since the request started.
    - **`cpu_time`**: CPU time consumed by the request.
    - **`reads` and `writes`**: Number of logical reads and writes, indicating I/O operations.

#### **D. Profiling with Extended Events or Query Store**

- **Extended Events**:
    - While not as straightforward in ADS as in SSMS, you can create and manage Extended Events sessions to capture detailed query execution metrics.

- **Query Store**:
    - Azure SQL Database and SQL Server have a **Query Store** feature that tracks query performance over time.
    - Use `sys.query_store_runtime_stats` and related DMVs to analyze historical performance data.

### **8. Best Practices for Further Optimization**

Based on the timing results and execution plans, consider the following optimization strategies:

1. **Index Optimization**:
    - Ensure all columns used in `WHERE`, `JOIN`, `ORDER BY`, and window functions are properly indexed.
    - Avoid unnecessary indexes that can slow down data manipulation operations.

2. **Minimize Temporary Table Usage**:
    - While temporary tables are useful, excessive use can tax `tempdb`.
    - Reuse temporary tables when possible or consider table variables for smaller datasets.

3. **Optimize String Operations**:
    - The `CHARINDEX` and `LEFT` functions used to extract `STREET_FIRST_TWO_WORDS` are necessary but ensure they are as efficient as possible.
    - Consider using more efficient string manipulation techniques if applicable.

4. **Leverage Set-Based Operations**:
    - Continue to prefer set-based operations over procedural or cursor-based approaches for handling large datasets.

5. **Parallelism Settings**:
    - Ensure SQL Server’s `MAXDOP` (Maximum Degree of Parallelism) setting is optimized to utilize available CPU cores effectively.

6. **Hardware and Resource Assessment**:
    - Verify that the SQL Server instance has sufficient CPU, memory, and disk I/O resources to handle the workload.
    - Consider upgrading resources or optimizing existing resource allocations if necessary.

### **9. Example Output Interpretation**

Suppose you execute the refactored script and receive the following output:

```
Step 1: Clean and Prepare Data took 30 seconds.
Step 2: Compute Previous Values Using Window Functions took 45 seconds.
Step 3: Flag Changes in Address took 20 seconds.
Step 4: Assign GroupAddressKey Using Cumulative Sum took 15 seconds.
Step 5: Aggregate Grouped Data took 10 seconds.
Step 6: Insert Aggregated Data into Target Table took 25 seconds.
Step 7: Cleanup Temporary Tables took 5 seconds.
```

**Analysis**:

- **Step 2 (Compute Previous Values)** is the longest, indicating that window functions may be a performance bottleneck.
- **Step 6 (Insert Aggregated Data)** also takes considerable time, suggesting that the `JOIN` operation or the target table's indexing might need optimization.
- **Steps 1 and 6** could be the focus areas for further optimization.

### **10. Conclusion**

By incorporating timing mechanisms directly into your SQL script, you gain granular insights into the performance of each step. This allows you to:

- **Identify Bottlenecks**: Pinpoint which operations consume the most time and resources.
- **Optimize Effectively**: Focus your optimization efforts on the most critical steps.
- **Monitor Progress**: Track performance improvements as you implement optimizations.

**Final Recommendations**:

1. **Implement the Timing Script**: Use the provided refactored script with timing measurements to gather execution times.
2. **Analyze and Optimize**: Based on the timing results, further refine your query, indexing strategy, and SQL Server configurations.
3. **Leverage Advanced Tools**: Utilize Azure Data Studio’s execution plans, extensions, and DMVs for deeper performance analysis.
4. **Iterative Testing**: Continuously test and iterate your optimizations to achieve the best possible performance.

Feel free to reach out if you need further assistance with specific optimization techniques or encounter any issues during implementation.


To mitigate the `tempdb` space issues by implementing batch processing based on `STUDY_ID`, we'll refactor your SQL script to process data in smaller, manageable chunks. This approach ensures that each batch consumes minimal `tempdb` resources, thereby avoiding the space allocation errors you've encountered.

Below is a comprehensive solution that includes:

1. **Batch Processing Using a Cursor**: Iterates through each `STUDY_ID` and processes data individually.
2. **Temporary Tables with Proper Indexing**: Utilizes temporary tables to handle intermediate data efficiently.
3. **Error Handling and Transaction Management**: Ensures that each batch is processed within its own transaction to maintain data integrity and free up `tempdb` resources promptly.
4. **Optimized Final Insert**: Inserts processed data into the target table without excessive `tempdb` usage.

## Step-by-Step Solution

### 1. Preparation: Understanding the Workflow

For each `STUDY_ID`, the following steps will be performed:

- **Data Cleaning and Transformation**: Clean street lines and extract the first two words.
- **Identify Address Changes**: Use window functions to detect changes in postal codes or streets.
- **Group Address Records**: Assign a `GroupAddressKey` based on detected changes.
- **Aggregate Data**: Summarize address information per group.
- **Insert into Target Table**: Join with the dimension table and insert the processed data.

### 2. Implementation: Refactored SQL Script

Here's the refactored SQL script that implements batch processing based on `STUDY_ID`:

```sql
-- =============================================
-- Step 1: Initialize Variables and Setup
-- =============================================

-- Declare variables for cursor
DECLARE @StudyID INT;

-- Define the batch size (number of STUDY_IDs to process per batch)
-- Adjust as necessary based on your environment and performance
DECLARE @BatchSize INT = 100;

-- Temporary table to hold STUDY_IDs to process
IF OBJECT_ID('tempdb..#StudyIDQueue') IS NOT NULL
    DROP TABLE #StudyIDQueue;

CREATE TABLE #StudyIDQueue (
    StudyID INT PRIMARY KEY
);

-- Populate the queue with STUDY_IDs from the dimension table
INSERT INTO #StudyIDQueue (StudyID)
SELECT DISTINCT StudyID
FROM [YourSchema].[StudyID_Dimension_Table] -- Replace with your actual dimension table
ORDER BY StudyID;

-- =============================================
-- Step 2: Setup Cursor for Batch Processing
-- =============================================

-- Declare cursor to iterate through STUDY_IDs
DECLARE StudyID_Cursor CURSOR FAST_FORWARD FOR
SELECT StudyID
FROM #StudyIDQueue;

OPEN StudyID_Cursor;

FETCH NEXT FROM StudyID_Cursor INTO @StudyID;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        -- Start a transaction for each STUDY_ID to ensure atomicity
        BEGIN TRANSACTION;
        
        -- =============================================
        -- Step 3: Process Data for the Current STUDY_ID
        -- =============================================

        -- Temporary table to hold cleaned data for the current STUDY_ID
        IF OBJECT_ID('tempdb..#CleanedData') IS NOT NULL
            DROP TABLE #CleanedData;

        SELECT
            STUDY_ID,
            POSTAL_CODE,
            STREET_LINE,
            CITY,
            dbo.clean_street_line(STREET_LINE, CITY) AS CleanedStreetLine,
            CAST(EFF_DATE AS DATE) AS EFF_DATE,
            CAST(END_DATE AS DATE) AS END_DATE
        INTO #CleanedData
        FROM [HealthFiles_test].[dev].[FCT_COMBINED_HEALTH_CLIENT_TABLE]
        WHERE STUDY_ID = @StudyID;

        -- Create index to optimize subsequent operations
        CREATE INDEX IX_CleanedData_Study_EffDate ON #CleanedData (STUDY_ID, EFF_DATE);

        -- Add STREET_FIRST_TWO_WORDS column
        ALTER TABLE #CleanedData
        ADD STREET_FIRST_TWO_WORDS NVARCHAR(255);

        -- Update STREET_FIRST_TWO_WORDS based on CleanedStreetLine and CITY
        UPDATE #CleanedData
        SET STREET_FIRST_TWO_WORDS = 
            CASE 
                WHEN CleanedStreetLine IS NOT NULL 
                     AND CITY IS NOT NULL 
                     AND LEN(CleanedStreetLine) > 0 THEN
                    CASE
                        WHEN CHARINDEX(' ', CleanedStreetLine) > 0 THEN
                            LEFT(
                                CleanedStreetLine,
                                CHARINDEX(' ', CleanedStreetLine + ' ', CHARINDEX(' ', CleanedStreetLine + ' ') + 1) - 1
                            )
                        ELSE CleanedStreetLine
                    END
                ELSE STREET_LINE
            END;

        -- Temporary table to hold lagged address data
        IF OBJECT_ID('tempdb..#LaggedAddressData') IS NOT NULL
            DROP TABLE #LaggedAddressData;

        SELECT
            CD.*,
            LAG(CD.POSTAL_CODE) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE) AS Prev_POSTAL_CODE,
            LAG(CD.STREET_FIRST_TWO_WORDS) OVER (PARTITION BY CD.STUDY_ID ORDER BY CD.EFF_DATE) AS Prev_STREET_FIRST_TWO_WORDS
        INTO #LaggedAddressData
        FROM #CleanedData CD;

        -- Create index to optimize window functions
        CREATE INDEX IX_LaggedAddressData_Study_EffDate ON #LaggedAddressData (STUDY_ID, EFF_DATE);

        -- Temporary table to hold change flags
        IF OBJECT_ID('tempdb..#ChangeFlagData') IS NOT NULL
            DROP TABLE #ChangeFlagData;

        SELECT
            LAD.*,
            CASE 
                WHEN Prev_POSTAL_CODE IS NULL 
                     OR Prev_STREET_FIRST_TWO_WORDS IS NULL 
                     OR Prev_POSTAL_CODE != POSTAL_CODE 
                     OR Prev_STREET_FIRST_TWO_WORDS != STREET_FIRST_TWO_WORDS 
                THEN 1 
                ELSE 0 
            END AS ChangeFlag
        INTO #ChangeFlagData
        FROM #LaggedAddressData LAD;

        -- Temporary table to hold grouped key data
        IF OBJECT_ID('tempdb..#GroupedKeyData') IS NOT NULL
            DROP TABLE #GroupedKeyData;

        SELECT
            CFD.*,
            SUM(ChangeFlag) OVER (PARTITION BY CFD.STUDY_ID ORDER BY CFD.EFF_DATE, CFD.ChangeFlag 
                ROWS UNBOUNDED PRECEDING) AS GroupAddressKey
        INTO #GroupedKeyData
        FROM #ChangeFlagData CFD;

        -- Temporary table to hold aggregated grouped data
        IF OBJECT_ID('tempdb..#GroupedData') IS NOT NULL
            DROP TABLE #GroupedData;

        SELECT
            STUDY_ID,
            POSTAL_CODE,
            STREET_FIRST_TWO_WORDS,
            MAX(STREET_LINE) AS STREET_LINE,
            MIN(EFF_DATE) AS EFF_DATE,
            MAX(END_DATE) AS END_DATE,
            GroupAddressKey
        INTO #GroupedData
        FROM #GroupedKeyData
        GROUP BY STUDY_ID, POSTAL_CODE, STREET_FIRST_TWO_WORDS, GroupAddressKey;

        -- =============================================
        -- Step 4: Insert Aggregated Data into Target Table
        -- =============================================

        -- Insert processed data into the target table with necessary joins
        INSERT INTO DEV.FCT_HEALTH_CLIENT_ADDRESS_DATE (
            STUDY_ID,
            GroupAddressKey,
            POSTAL_CODE,
            STREET_LINE,
            EFF_DATE,
            END_DATE,
            CITY,
            LATITUDE,
            LONGITUDE
        )
        SELECT 
            A.STUDY_ID,
            A.GroupAddressKey,
            A.POSTAL_CODE,
            A.STREET_LINE,
            A.EFF_DATE,
            A.END_DATE,       
            B.[CITY], 
            B.LATITUDE, 
            B.LONGITUDE
        FROM #GroupedData A
        LEFT JOIN dev.DIM_HEALTH_CLIENT_ADDRESS B
            ON A.POSTAL_CODE = B.POSTAL_CODE 
            AND A.STREET_LINE = B.STREET_LINE;

        -- =============================================
        -- Step 5: Clean Up Temporary Tables
        -- =============================================

        DROP TABLE IF EXISTS #CleanedData, #LaggedAddressData, #ChangeFlagData, #GroupedKeyData, #GroupedData;

        -- Commit the transaction for the current STUDY_ID
        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH
        -- Rollback transaction in case of error
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Capture and log the error details
        DECLARE @ErrorMessage NVARCHAR(4000), @ErrorSeverity INT, @ErrorState INT;
        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();

        -- Log the error (you can customize this part to log into a table or other logging mechanism)
        PRINT 'Error processing STUDY_ID ' + CAST(@StudyID AS NVARCHAR) + ': ' + @ErrorMessage;

        -- Optionally, exit the loop or continue with the next STUDY_ID
        -- For this example, we'll continue
    END CATCH

    -- Fetch the next STUDY_ID
    FETCH NEXT FROM StudyID_Cursor INTO @StudyID;
END

-- Close and deallocate the cursor
CLOSE StudyID_Cursor;
DEALLOCATE StudyID_Cursor;

-- Clean up the queue table
DROP TABLE IF EXISTS #StudyIDQueue;

-- =============================================
-- Step 6: Finalizing
-- =============================================

-- Optionally, add indexes to the target table if necessary
-- CREATE INDEX IX_FCT_HEALTH_CLIENT_ADDRESS_DATE_StudyID ON DEV.FCT_HEALTH_CLIENT_ADDRESS_DATE (STUDY_ID, GroupAddressKey);

PRINT 'Batch processing completed successfully.';
```

### 3. Explanation of the Refactored Script

#### **Step 1: Initialization**

- **Variables and Queue Setup**: 
  - `@StudyID`: Holds the current `STUDY_ID` being processed.
  - `@BatchSize`: Defines how many `STUDY_IDs` to process in one batch. Although in this script we're processing one `STUDY_ID` at a time, you can adjust this to process multiple `STUDY_IDs` per batch if desired.
  - `#StudyIDQueue`: A temporary table that holds all distinct `STUDY_IDs` to be processed, sourced from your `STUDY_ID` dimension table.

#### **Step 2: Cursor Declaration**

- **Cursor `StudyID_Cursor`**: Iterates through each `STUDY_ID` in the queue. The `FAST_FORWARD` option optimizes the cursor for read-only, forward-only operations, which is suitable for this scenario.

#### **Step 3: Processing Each `STUDY_ID`**

For each `STUDY_ID`:

1. **Begin Transaction**: Ensures that all operations for the current `STUDY_ID` are atomic. If any step fails, the transaction is rolled back to maintain data integrity.

2. **Data Cleaning and Transformation**:
   - **`#CleanedData` Temporary Table**: Holds raw data filtered by the current `STUDY_ID` and applies the `dbo.clean_street_line` function.
   - **Indexing**: Creates an index on `STUDY_ID` and `EFF_DATE` to optimize subsequent window functions.
   - **`STREET_FIRST_TWO_WORDS` Calculation**: Extracts the first two words from the cleaned street line.

3. **Identify Address Changes**:
   - **`#LaggedAddressData` Temporary Table**: Utilizes the `LAG` function to fetch previous postal codes and street segments for comparison.
   - **Indexing**: Similar indexing to optimize window functions.

4. **Flagging Changes**:
   - **`#ChangeFlagData` Temporary Table**: Assigns a `ChangeFlag` indicating whether there's a change in postal code or street segment compared to the previous record.

5. **Grouping Address Records**:
   - **`#GroupedKeyData` Temporary Table**: Uses a cumulative `SUM` over `ChangeFlag` to assign a `GroupAddressKey`. This key uniquely identifies a group of continuous address records without changes.
   
6. **Aggregating Data**:
   - **`#GroupedData` Temporary Table**: Aggregates data based on `STUDY_ID`, `POSTAL_CODE`, `STREET_FIRST_TWO_WORDS`, and `GroupAddressKey`. It captures the earliest `EFF_DATE`, latest `END_DATE`, and the maximum `STREET_LINE`.

7. **Inserting into Target Table**:
   - **Final Insert**: Joins the aggregated data with the `DIM_HEALTH_CLIENT_ADDRESS` table to fetch additional details like `CITY`, `LATITUDE`, and `LONGITUDE`, and inserts the result into `DEV.FCT_HEALTH_CLIENT_ADDRESS_DATE`.

8. **Clean-Up**:
   - **Dropping Temporary Tables**: Frees up `tempdb` resources by dropping temporary tables after processing each `STUDY_ID`.
   
9. **Error Handling**:
   - **`TRY...CATCH` Block**: Ensures that any errors during processing are caught, transactions are rolled back, and errors are logged without halting the entire batch process.

#### **Step 4: Finalization**

- **Closing and Deallocating the Cursor**: Properly releases cursor resources.
- **Dropping the Queue Table**: Cleans up the temporary queue table used for `STUDY_IDs`.
- **Final Message**: Prints a completion message upon successful processing.

### 4. Additional Optimizations and Recommendations

#### **A. Indexing the Target Table**

After inserting all data, consider adding indexes to the `DEV.FCT_HEALTH_CLIENT_ADDRESS_DATE` table to optimize query performance for future operations:

```sql
CREATE INDEX IX_FCT_HEALTH_CLIENT_ADDRESS_DATE_StudyID 
ON DEV.FCT_HEALTH_CLIENT_ADDRESS_DATE (STUDY_ID, GroupAddressKey);
```

#### **B. Adjusting Batch Size**

The current script processes one `STUDY_ID` at a time. Depending on the number of records per `STUDY_ID` and your server's capacity, you might benefit from processing multiple `STUDY_IDs` in a single transaction. To do this:

1. **Modify the Cursor to Fetch Multiple `STUDY_IDs` per Batch**:

   Replace the `@BatchSize` variable usage with logic to fetch a set number of `STUDY_IDs` per iteration.

2. **Process the Batch Within a Single Transaction**:

   Ensure that all `STUDY_IDs` in the batch are processed together, adjusting temporary table handling accordingly.

**Note**: Be cautious with larger batch sizes as they may still lead to `tempdb` pressure. Monitor performance and adjust as needed.

#### **C. Optimize `dbo.clean_street_line` Function**

Ensure that the `dbo.clean_street_line` function is optimized:

- **Inline Table-Valued Function**: If it's a scalar function, consider converting it to an inline table-valued function for better performance.
- **Simplify Logic**: Review and simplify the function's logic to reduce computational overhead.

#### **D. Monitor `tempdb` Usage**

Continuously monitor `tempdb` to ensure that the batch processing does not exceed available space:

```sql
SELECT 
    SUM(unallocated_extent_page_count) AS [Free Pages],
    SUM(user_object_reserved_page_count) AS [User Object Pages],
    SUM(internal_object_reserved_page_count) AS [Internal Object Pages],
    SUM(version_store_reserved_page_count) AS [Version Store Pages],
    SUM(unallocated_extent_page_count + user_object_reserved_page_count + internal_object_reserved_page_count + version_store_reserved_page_count) AS [Total Pages]
FROM sys.dm_db_file_space_usage;
```

#### **E. Optimize Final Insert**

- **Remove `ORDER BY`**: The final `ORDER BY` clause in the `INSERT` statement can consume additional `tempdb` space for sorting. Since the order is not required during insertion, it's best to omit it. You can apply ordering later when querying the target table.

   ```sql
   -- Remove ORDER BY from the final insert
   INSERT INTO DEV.FCT_HEALTH_CLIENT_ADDRESS_DATE (
       STUDY_ID,
       GroupAddressKey,
       POSTAL_CODE,
       STREET_LINE,
       EFF_DATE,
       END_DATE,
       CITY,
       LATITUDE,
       LONGITUDE
   )
   SELECT 
       A.STUDY_ID,
       A.GroupAddressKey,
       A.POSTAL_CODE,
       A.STREET_LINE,
       A.EFF_DATE,
       A.END_DATE,       
       B.[CITY], 
       B.LATITUDE, 
       B.LONGITUDE
   FROM #GroupedData A
   LEFT JOIN dev.DIM_HEALTH_CLIENT_ADDRESS B
       ON A.POSTAL_CODE = B.POSTAL_CODE 
       AND A.STREET_LINE = B.STREET_LINE;
   ```

- **Post-Insertion Indexing**: After bulk inserting all data, create non-clustered indexes to improve query performance.

#### **F. Parallel Processing**

If your server resources allow, consider parallelizing the batch processing by running multiple instances of the script simultaneously, each handling a subset of `STUDY_IDs`. **Caution**: Ensure that this does not lead to `tempdb` contention or other resource bottlenecks.

#### **G. Cleanup and Maintenance**

- **Regularly Rebuild Indexes**: To maintain optimal performance, regularly rebuild or reorganize indexes on the target table.
- **Archive or Purge Old Data**: Implement data retention policies to archive or purge obsolete data, freeing up space in `tempdb` and the target database.

### 5. Summary

By implementing batch processing based on `STUDY_ID`, you can significantly reduce the load on `tempdb`, thereby preventing the space allocation errors you previously encountered. This approach ensures that each batch is processed efficiently and independently, maintaining data integrity and optimizing resource usage.

**Key Benefits of This Approach**:

- **Reduced `tempdb` Usage**: Processing one `STUDY_ID` at a time limits the amount of temporary data held in `tempdb`.
- **Improved Error Handling**: Transactions per batch ensure that errors in one `STUDY_ID` do not affect others.
- **Scalability**: Easily adjustable batch sizes allow the process to scale based on available resources.
- **Maintainability**: Clear separation of processing logic per `STUDY_ID` enhances the script's readability and maintainability.

**Next Steps**:

1. **Test in Development Environment**: Before deploying to production, thoroughly test the script in a development or staging environment to ensure it behaves as expected.
2. **Monitor Performance**: Use SQL Server monitoring tools to observe the performance and `tempdb` usage during execution.
3. **Iterate and Optimize**: Based on test results, further refine batch sizes, indexing strategies, and function optimizations to achieve the best performance.

Feel free to reach out if you encounter any issues or need further assistance with specific aspects of this solution.