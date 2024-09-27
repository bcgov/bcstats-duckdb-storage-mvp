## Overview of dbt-duckdb

This project is designed to load multiple CSV files into a DuckDB database using **dbt** (Data Build Tool). It helps automate the process of transforming, modeling, and querying data from CSV files in a simple and efficient way. DuckDB is used as the local database engine, and dbt orchestrates the loading and transformation process.



## Features


=======
- **Automated CSV Loading:** Easily load multiple CSV files into DuckDB.
- **Data Transformation:** Use dbt to perform SQL-based data transformations on the loaded CSVs.
- **Lightweight Database:** Leverages DuckDB, which is optimized for efficient data processing on local files.
- **Flexible:** Customize dbt models to suit your business logic and transformation requirements.

---

## Requirements

Since we use `dbt-duckdb` Python package, before starting, ensure you have the following tools installed:


1. **Python** (>= 3.7)
2. **dbt** (>= 0.19.0)
3. **DuckDB** (Python package or standalone)
4. **CSV Files:** Ensure you have your source CSV files ready.


To install the required Python packages:

```bash
pip install dbt-duckdb duckdb
```

More instructions are available in https://github.com/mehd-io/dbt-duckdb-tutorial/

---

## Setup

Follow these steps to set up and configure the project:

1. **Clone the repository:**

   ```bash
   git clone <your-repository-url>
   cd <project-folder>
   ```

2. **Initiate a new projec:**

If you start a new project, 

```bash
dbt init
```
It will:

    ask you to name your project
    ask you which database adapter you're using (or to Supported Data Platforms)
    prompt you for each piece of information that dbt needs to connect to that database: things like account, user, password, etc

Then, it will:

    Create a new folder with your project name and sample files, enough to get you started with dbt
    Create a connection profile on your local machine. The default location is ~/.dbt/profiles.yml.


3. **Configure dbt profiles:**

   dbt uses a `profiles.yml` file to connect to DuckDB. Ensure that your `profiles.yml` file is correctly configured as follows:

   ```yaml
   duckdb_project:
     target: dev
     outputs:
       dev:
         type: duckdb
         path: "path/to/your/database.db"  # Path to your DuckDB database file
         extensions: ['parquet', 'json']   # Optional extensions to load, if needed
            duckdb_project:

       test:
         type: duckdb
         path: "test.db"  # Test database path
         
       prod:
         type: duckdb
         path: "prod.db"  # Production database path
   ```

4. **Set up the directory structure:**

   Your dbt project should follow this structure:

   ```
   .
   ├── models
   │   ├── staging
   │   │   └── csv_imports.sql  # SQL to define CSV imports
   │   ├── transforms.sql       # SQL transformation models
   |   ├── source.yml  
   ├── data
   │   └── source.csv           # Place your CSV files here
   ├── dbt_project.yml          # dbt project configuration
   └── README.md                # This file
   ```

---


## Usage

### Loading CSV Files

1. **Prepare your CSV files:**
   - Place the source CSV files inside the `data/` directory.

2. **Create staging models:**
   - In the `models/staging` folder, create a model (SQL file) that defines how each CSV should be loaded into DuckDB.

   Example SQL to load a CSV:

   ```sql
   {{ config(materialized='table') }}

   select * 
   from read_csv_auto('data/source.csv');
   ```

3. **Run dbt to load and transform data:**

   Once your staging models are ready, run dbt to load the CSVs into DuckDB:

   ```bash
   dbt run
   ```

4. **Query the data:**

   After running dbt, you can query your data directly using DuckDB or through dbt models.

   Example:

   ```sql
   select * from staging.source_csv;
   ```

---

## Example

Here is an example walkthrough:

1. Place a CSV file, `sales_data.csv`, in the `data/` folder.
2. Create a corresponding SQL file, `sales_data.sql`, in the `models/staging/` folder:

   ```sql
   {{ config(materialized='table') }}

   select * 
   from read_csv_auto('data/sales_data.csv');
   ```

3. Run dbt to load the data:

   ```bash
   dbt run
   ```

4. Query the loaded `sales_data` table:

   ```sql
   select * from staging.sales_data;
   ```


More instructions can be found from the documents in a dbt starter project:

### Using the starter project

Try running the following commands:
- dbt run
- dbt test

1.	A dbt workflow has been established in “dbt”
    a.	The basic setup follows : https://github.com/mehd-io/dbt-duckdb-tutorial/tree/main and https://www.youtube.com/watch?v=asxGh2TrNyI
        i.	This project use the dbt-duckdb adapter for DuckDB. You can install it by doing `pip install dbt-duckdb`. This include `dbt`, `dbt-duckdb adapter` and `duckdb`.
        ii.	Inside the dbt project /dbt,  run `dbt run`
        iii.	It will implement the models in “models” folder and create tables or output CSV files.

2.	A GitHub action has been established in the workflow.yaml file to run the dbt project. This is to ensure the project can be run in a CI/CD pipeline. Not test yet.

3. Now a fin-neighborhood-incomes-2021.csv file has been added to the external_source folder. This is to test the ability to read from a CSV file. After the staging, this table should be stacked with fin-neighborhood-incomes-2016-2020.csv. 

4. The table_98_401_X2021025_English_CSV_data.csv file has been added to the external_source folder. This is to test the ability to read from a CSV file. After the staging, this table should be stacked with table_98_401_X2021006_English_CSV_data_BritishColumbia.csv. 


Let’s walk through the steps of creating **test** and **production** databases for your dbt + DuckDB setup, and the best practices around managing staging tables, test databases, and production databases.

### 1. **Creating Test and Production Databases**

Since you’re using **DuckDB** as your database engine, which is a file-based database, creating separate **test** and **production** databases involves creating separate **DuckDB database files** for each environment. You can do this by either:
- **Copying** your existing development database to new files (for test and prod), or
- **Rebuilding** the test and production databases from scratch using dbt.

Here are the steps for both approaches:

#### Option 1: **Copying the dev database**
If your dev environment has been fully built and you want to quickly replicate that into test and production, you can copy the `.db` file for the DuckDB database:

1. **Locate the dev database file**: Typically, the database file would be something like `dev.db` or whatever you specified in the `profiles.yml` file for the `dev` environment.

2. **Create copies** for test and production:
   ```bash
   cp dev.db test.db
   cp dev.db prod.db
   ```

3. **Update `profiles.yml`**: You'll need to modify the `profiles.yml` file to point to these new database files for the test and production environments.

   Example `profiles.yml` configuration:
   ```yaml
   duckdb_project:
     target: dev
     outputs:
       dev:
         type: duckdb
         path: "dev.db"  # Dev database path
       test:
         type: duckdb
         path: "test.db"  # Test database path
       prod:
         type: duckdb
         path: "prod.db"  # Production database path
   ```

4. **Switch between environments**: You can now switch between environments (dev, test, prod) using the `--target` flag in dbt commands.

   For example:
   ```bash
   dbt run --target test  # To run models on the test database
   dbt run --target prod  # To run models on the production database
   ```

#### Option 2: **Rebuilding test and production from scratch**
Alternatively, you can build the test and production databases from scratch by running your dbt models directly on a fresh database for each environment.

1. **Create new database files** (optional): If you want to start with empty databases for test and production, simply specify a new `.db` file path in your `profiles.yml`.

2. **Run dbt for each environment**:
   ```bash
   dbt run --target test  # Build the models in the test environment
   dbt run --target prod  # Build the models in the production environment
   ```

### 2. **Handling Staging Tables**

#### Should You Drop Staging Tables in Test and Production?

In general, **staging tables** are meant to hold raw or lightly transformed data from external sources. Whether or not to keep them in the test and production environments depends on your use case and the following factors:

- **Staging tables as temporary/ephemeral data**: If the staging tables are only needed as intermediate steps in the transformation process (i.e., they are not used directly for reporting or querying), you can **materialize them as ephemeral** or **drop them** after building the final transformed tables.
  
  - **Ephemeral materialization**: In dbt, you can configure staging models to be **ephemeral**. Ephemeral models do not get materialized as tables; instead, they are treated as inline subqueries within downstream models.
    ```yaml
    {{ config(materialized='ephemeral') }}
    ```

  - **Dropping staging tables**: If your staging tables are materialized as physical tables (e.g., in the `dev` environment), and you no longer need them after generating the final tables in test/production, you can drop them manually using SQL commands, or rely on `ephemeral` for environments where persistence is not required.

#### Best Practice for Staging Tables in Production:
- **Keep only essential tables**: For the production environment, it's often best to keep only the **final transformed or stacked tables** that are required for business operations, reporting, or downstream applications.
  
  You can run a cleanup process after the `dbt run` to drop the staging tables:
  
  ```sql
  drop table if exists staging_table_name;
  ```

### 3. **Handling Staging and Transformation in Each Environment**

Here’s a breakdown of what you can do in each environment:

#### **Development Environment**:
- **All tables**: Keep both staging and transformed tables, as you might need to debug, develop, or refine your transformations based on raw/staged data.
- **Iterate quickly**: The dev environment is where you should freely run dbt commands, re-run models, and experiment.

#### **Test Environment**:
- **Replicate prod setup**: The test environment should mirror the production environment as closely as possible to allow for testing the entire pipeline before promoting to production.
- **Keep staging models if needed**: If testing involves verifying raw data or intermediate transformations, you may want to keep the staging models. Otherwise, you can drop them after testing.
- **Data validation**: Use this environment to run all your **dbt tests** to ensure data integrity.

#### **Production Environment**:
- **Final tables only**: Keep only the final transformed tables that are needed for reporting, analytics, or downstream systems. Staging tables that are not needed should be dropped.
- **Optimize for efficiency**: Use **ephemeral** models for staging in production if the raw data is only needed for transformation, and avoid persisting unnecessary intermediate tables.

### 4. **dbt Materialization and Configuration in Each Environment**

dbt allows you to configure materializations differently for each environment. For example, you can use different materialization strategies for the staging tables in dev, test, and prod:

- In **development**, you might want to materialize staging tables as physical tables (for easy debugging):
  ```sql
  {{ config(materialized='table') }}
  ```

- In **production**, you can configure them to be ephemeral (so they are not persisted):
  ```yaml
  models:
    staging:
      +materialized: ephemeral  # Use ephemeral materialization for staging in production
  ```

This way, you can retain staging tables in the dev environment but avoid persisting them in the test and production environments, where only the final tables are needed.

### Conclusion

- **Creating test and prod databases**: You can either copy your dev database file or rebuild the test and production databases using dbt in separate files (e.g., `test.db` and `prod.db`).
- **Managing staging tables**:
  - In **dev**, keep all staging tables to allow debugging and development.
  - In **test**, keep staging tables as needed for testing but consider dropping them after validation.
  - In **production**, keep only the final transformed tables. You can drop or make staging tables ephemeral to avoid clutter and improve performance.

This approach ensures that your test and production environments are clean, optimized, and aligned with best practices for efficient data pipeline management.



### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
