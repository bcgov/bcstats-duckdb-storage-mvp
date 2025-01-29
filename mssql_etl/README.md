## # Health Client Roster Management System

## Table of Contents

- [# Health Client Roster Management System](#-health-client-roster-management-system)
- [Table of Contents](#table-of-contents)
- [Overview](#overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Database Objects](#database-objects)
  - [Views](#views)
  - [Stored Procedures](#stored-procedures)
- [Setup and Deployment](#setup-and-deployment)
  - [Version Control](#version-control)
  - [Using SQL Server Data Tools (SSDT)](#using-sql-server-data-tools-ssdt)
- [Usage](#usage)
  - [Executing Stored Procedures](#executing-stored-procedures)
- [Testing](#testing)
  - [Unit Tests](#unit-tests)
  - [Integration Tests](#integration-tests)

## Overview

The **Health Client Roster Management System** is a robust solution designed to manage and maintain a centralized roster of health clients. It consolidates data from multiple monthly tables, ensuring data integrity, consistency, and accessibility for reporting and analysis. The system leverages Microsoft SQL Server's advanced features, including views and stored procedures, to provide an efficient and scalable data management framework.

## Features

- **Centralized Data Consolidation**: Aggregates data from various monthly sources into a unified view.
- **Dimension Management**: Maintains separate dimension tables for client IDs and addresses, ensuring normalization.
- **Incremental Updates**: Utilizes stored procedures with `MERGE` statements for efficient data synchronization.
- **Automated Scheduling**: Facilitates automated execution of updates using SQL Server Agent.
- **Version Control Integration**: Organizes database objects in a structured folder hierarchy compatible with version control systems.
- **Error Handling and Logging**: Implements robust error handling within stored procedures to ensure data integrity.
- **Scalability**: Designed to accommodate growing datasets and additional monthly data sources seamlessly.

## Prerequisites

- **Microsoft SQL Server**: Version 2016 or later is recommended.
- **SQL Server Management Studio (SSMS)**: For database management and script execution.
- **Version Control System (VCS)**: Git is preferred for managing scripts and database objects.
- **SQL Server Data Tools (SSDT)** *(Optional)*: For integrated database project management within Visual Studio.
- **Access Permissions**: Sufficient permissions to create and alter database objects, execute stored procedures, and set up SQL Server Agent jobs.

## Project Structure

The project is organized into a clear and logical folder hierarchy to facilitate easy navigation, maintenance, and collaboration. Each type of database object is stored in its respective folder, adhering to consistent naming conventions.

/HealthClientRosterProject
│
├── /StoredProcedures
│   ├── ID
│   │   ├── SP_Update_DIM_HEALTH_CLIENT_ID.sql
│   │   └── SP_Update_DIM_HEALTH_CLIENT_ADDRESS.sql
│   ├── Address
│   │   ├── SP_Update_DIM_HEALTH_CLIENT_ADDRESS.sql
│   │   └── SP_Update_DIM_HEALTH_CLIENT_ADDRESS.sql
│   └── Others
│       ├── SP_Update_HEALTH_CLIENT_Dimensions.sql
│       └── SP_Update_HEALTH_CLIENT_Dimensions.sql
│
├── /Functions
│   └── x
│       └── FN_x.sql
│
├── /Views
│   ├── VIEW_COMBINED_HEALTH_CLIENT.sql
│   ├── VIEW_COMBINED_HEALTH_CLIENT_ID.sql
│   └── VIEW_COMBINED_HEALTH_CLIENT_ADDRESS.sql
│
├── /Triggers
│   ├── Trg_x.sql
│   └── Trg_x.sql
│
├── /Initials
│   ├── MT_FCT_COMBINED_HEALTH_CLIENT.sql
│   ├── DIM_COMBINED_HEALTH_CLIENT_ID.sql
│   ├── DIM_COMBINED_HEALTH_CLIENT_ADDRESS.sql
│   └── FCT_COMBINED_HEALTH_CLIENT_ADDRESS_DATE.sql
│
├── /Indexes
│   ├── x.sql
│   └── x.sql
│
├── /Scripts
│   ├── FCT_HEALTH_CLIENT_ADDRESS_DATE_BATCH_PROCESSING.sql
│   ├── generate_sql_query_for_healthclientfile_in_R.r
│   └── x.sql
│
├── /Documentation
│   ├── README.md
│   ├── Design_Documentation.md
│   └── Change_Log.md
│
├── /Tests
│   ├── UnitTests.sql
│   └── IntegrationTests.sql
│
└── .gitignore



 
## Database Objects

### Views

- **VIEW_COMBINED_HEALTH_CLIENT**
  - **Purpose**: Consolidates data from all monthly `CLR_EXT_%` tables into a unified view.
  - **Location**: `/Views/VIEW_COMBINED_HEALTH_CLIENT.sql`

- **VIEW_COMBINED_HEALTH_CLIENT_ID**
  - **Purpose**: Extracts distinct `STUDY_ID`, `BIRTH_YR_MON`, and `SEX` combinations for the ID dimension.
  - **Location**: `/Views/VIEW_COMBINED_HEALTH_CLIENT_ID.sql`

- **VIEW_COMBINED_HEALTH_CLIENT_ADDRESS**
  - **Purpose**: Aggregates address-related information (`POSTAL_CODE`, `STREET_LINE`, `CITY`, etc.) for the Address dimension.
  - **Location**: `/Views/VIEW_COMBINED_HEALTH_CLIENT_ADDRESS.sql`

### Stored Procedures

- **SP_Update_DIM_HEALTH_CLIENT_ID**
  - **Purpose**: Performs incremental updates on the `DEV.DIM_HEALTH_CLIENT_ID` table using the `MERGE` statement.
  - **Location**: `/StoredProcedures/SP_Update_DIM_HEALTH_CLIENT_ID.sql`

- **SP_Update_DIM_HEALTH_CLIENT_ADDRESS**
  - **Purpose**: Performs incremental updates on the `DEV.DIM_HEALTH_CLIENT_ADDRESS` table using the `MERGE` statement.
  - **Location**: `/StoredProcedures/SP_Update_DIM_HEALTH_CLIENT_ADDRESS.sql`

- **SP_Update_HEALTH_CLIENT_Dimensions**
  - **Purpose**: Updates both `VIEW_COMBINED_HEALTH_CLIENT_ID` and `VIEW_COMBINED_HEALTH_CLIENT_ADDRESS` views.
  - **Location**: `/StoredProcedures/SP_Update_HEALTH_CLIENT_Dimensions.sql`

## Setup and Deployment

### Version Control

All SQL scripts and database objects are managed using **Git**. This ensures version tracking, collaboration, and rollback capabilities.

1. **Initialize Repository**:
   - Navigate to the project directory.
   - Initialize a Git repository:
     ```bash
     git init
     ```

2. **Add Remote Repository**:
   - Connect to a remote repository (e.g., GitHub, GitLab):
     ```bash
     git remote add origin https://github.com/yourusername/HealthClientRosterProject.git
     ```

3. **Commit Changes**:
   - Stage and commit all files:
     ```bash
     git add .
     git commit -m "Initial commit of Health Client Roster Management System"
     ```

4. **Push to Remote**:
   - Push commits to the remote repository:
     ```bash
     git push -u origin master
     ```

### Using SQL Server Data Tools (SSDT)

For enhanced development experience and integration with Visual Studio:

1. **Install SSDT**:
   - Download and install SSDT from [Microsoft](https://docs.microsoft.com/en-us/sql/ssdt/download-sql-server-data-tools-ssdt).

2. **Create a Database Project**:
   - Open Visual Studio.
   - Create a new **SQL Server Database Project**.
   - Import existing database schemas or add scripts from the project structure.

3. **Build and Deploy**:
   - Use SSDT's build and publish features to deploy changes to different environments.

## Usage

### Executing Stored Procedures

To perform incremental updates on the ID and Address dimension tables, execute the respective stored procedures.

1. Update Both Dimensions View:

```sql
EXEC DEV.SP_Update_HEALTH_CLIENT_Dimensions;
```

2. **Update ID Dimension Table**:
   ```sql
   EXEC DEV.SP_Update_DIM_HEALTH_CLIENT_ID;
   ```


3. **Update Address Dimension Table**:

```sql
EXEC DEV.SP_Update_DIM_HEALTH_CLIENT_ADDRESS;
```

## Testing
Ensure that your database objects function as expected through comprehensive testing.

### Unit Tests
Objective: Validate individual stored procedures and functions for correct behavior.
Location: /Tests/UnitTests.sql
Example:
```sql
-- Test SP_Update_DIM_HEALTH_CLIENT_ID
EXEC DEV.SP_Update_DIM_HEALTH_CLIENT_ID;

-- Verify results
SELECT * FROM DEV.DIM_HEALTH_CLIENT_ID WHERE STUDY_ID = 'SampleID';
```

### Integration Tests
Objective: Ensure that multiple database objects work together seamlessly.
Location: /Tests/IntegrationTests.sql
Example:


```sql

-- Execute Stored Procedures
EXEC DEV.SP_Update_HEALTH_CLIENT_Dimensions;

-- Validate Address Updates
SELECT * FROM DEV.DIM_HEALTH_CLIENT_ADDRESS WHERE CITY = 'SampleCity';
```
### TODO

The EFF_DATE and END_DATE in CLR_EXT_20230828 and the tables after it are messy. Provide date long time before the 20230828
, such as 20210128
