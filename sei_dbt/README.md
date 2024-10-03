## Overview

This project is created to load multiple CSV files into a DuckDB database using **dbt-duckdb** (Data Build Tool). It helps automate the process of transforming, modeling, and querying data from CSV files in a simple and efficient way. DuckDB is used as the local database engine, and dbt orchestrates the loading and transformation process.



## Rationale

- **Automated CSV Loading:** Easily load multiple CSV files into DuckDB.
- **Data Transformation:** Use dbt to perform SQL-based data transformations on the loaded CSVs.
- **Lightweight Database:** Leverages DuckDB, which is optimized for efficient data processing on local files.
- **Flexible:** Customize dbt models to suit our business logic and transformation requirements.

---

## Requirements

Since we use `dbt-duckdb` Python package, before starting, ensure you have the following tools installed:


1. **Python** (>= 3.7)
2. **dbt** (>= 0.19.0)
3. **DuckDB** (Python package or standalone)
4. **CSV Files:** Ensure you have your source CSV files ready.




More instructions are available in https://github.com/mehd-io/dbt-duckdb-tutorial/

---

## Setup

Follow these steps to set up and configure the project:

1. **Clone the repository:**

   ```bash
   git clone <your-repository-url>
   cd <project-folder>
   ```
   
2. **Install the required Python packages**

After you clone the repo, make sure there is `requirment.txt` file.

In this project, we recommend using a **Conda environment** to manage dependencies and keep your Python environments clean. 

To get started, create a new Conda environment from the command line with the following command: `conda create -n <env_name> python=<version>`. 

Once the environment is created, activate it with `conda activate <env_name>`. 


You can then install all necessary dependencies within this environment using


```bash
pip install -r requirements.txt
```

If you already have a python environment to work with, you can

```bash
conda install dbt-duckdb
conda install duckdb
```

Every time you work on the project, ensure the environment is active by running `conda activate <env_name>` to access the correct setup. 

This approach isolates dependencies, minimizing conflicts across projects.
   
   

3. **Initiate a new project:**

If you start a new project from scratch, you can

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
    
If you clone this repo, it should already be initiated, and all the folders and files are already created.


3. **Configure dbt profiles:**

The `profiles.yml` file is designed for **global or environment-specific configurations** and often contains sensitive information like **database credentials**, **connection settings**, and **environment-specific settings**. Unlike `dbt_project.yml`, this file is stored outside the project directory, often in the root directory or the user's home directory (`~/.dbt/` by default). This keeps sensitive information like passwords and credentials out of version control.

   ```yaml
   duckdb_project:
     target: dev
     outputs:
       dev:
         type: duckdb
         path: "path/to/your/database.db"  # Path to your DuckDB database file
         extensions: ['parquet', 'json']   # Optional extensions to load, if needed
            duckdb_project:
               threads: 4
         external_root: "data/dev"

       test:
         type: duckdb
         path: "test.db"  # Test database path
         external_root: "data/test" 
         
       prod:
         type: duckdb
         path: "prod.db"  # Production database path
         external_root: "data/prod"
   ```
   
In this `profiles.yml`:
- We have three environments: `dev`, `test` and `prod`.
- For each environment, we define:
  - `external_root`: A variable points to the directory where CSV files or other data are located. This is important since in this project, most of the CSV files are stored on the LAN whose path should not be hard-code and we need `external_root` to store this path information.
  - `dbt-duckdb` only support certain keys in the `profiles.yml`; for example, `external_root`, `settings`, and `config_options`, etc, and each has its own purpose. `custom`, `vars` are not supported in `profiles.yml`, but are supported in `dbt-project.yml`. 
   
#### Key Usage:
- **Sensitive Information**: The `profiles.yml` file is the right place to store sensitive information like database credentials, API tokens, or connection settings. It is generally **not version-controlled**, as it may contain secrets. In this project, 
- **Multiple Environments**: You can define multiple environments (like `dev`, `prod`, `test`) within `profiles.yml` and switch between them using the `target` option. This is useful when different teams or environments need to connect to different databases or use different credentials.

#### Default Location:
- The `profiles.yml` file is located by default in `~/.dbt/profiles.yml`, making it global across multiple dbt projects on your local machine. If you use a Windows machine, it could be in `C:\Users\myusername\.dbt`.
- You can also override this by explicitly specifying the location of `profiles.yml` with the `DBT_PROFILES_DIR` environment variable.
- In this project, 


To **infer variables from `profiles.yml`** in your **dbt model file**, you need to use **Jinja** templating and the `target` object, which exposes information from `profiles.yml`.


#### Step 2: Access the Variables in the Model File


Here’s an example of how you might use the `data_path` variable inside a dbt model:

```sql
-- models/staging/my_staging_model.sql

WITH raw_data AS (

    -- Use the data_path from profiles.yml
    SELECT *
    FROM read_csv_auto('{{ target.external_root }}/my_file.csv')

),

final_data AS (

    SELECT
        *
    FROM raw_data

)

SELECT * FROM final_data;
```

- **`{{ target.vars.data_path }}`**: This Jinja expression accesses the `data_path` variable from `profiles.yml`. When you run dbt with the `--target dev` flag, it will dynamically insert `"data/dev"`, and for `--target prod`, it will insert `"data/prod"`.

In this case, the model will:
1. **Use the `data_path`** defined in `profiles.yml` to load the appropriate CSV file based on the environment (e.g., `data/dev/my_file.csv` in dev, `data/prod/my_file.csv` in prod).


#### Source:
- [Profiles.yml Reference](https://docs.getdbt.com/docs/configure-your-profile)
   
   
4. **Configure project:**   Project-Specific Configuration

The `dbt_project.yml` file is the configuration file specific to a **single dbt project**. It contains settings related to that particular project, such as the project's name, paths to models, seeds, and specific **variables** (used in transformations and models).

This file is usually placed in the root of the dbt project directory and is included in version control because it’s tied to the project itself.

#### Example of `dbt_project.yml`:
```yaml
name: 'my_dbt_project'
version: '1.0.0'
profile: 'my_profile'

# Define the structure and directories for models
model-paths: ["models"]
seed-paths: ["seeds"]

# Project-specific variables
vars:
  some_variable: 'value'
  environment: 'dev'  # Example environment variable
```

#### Key Usage:
- **Variables**: You can define project-specific variables under `vars`. These are useful when you need to reference configuration values in multiple places in your models, macros, or tests.
  - These variables are **project-specific** and are **not meant to store sensitive information** like database credentials.
- **Project Paths**: You can configure paths for models, seeds, and snapshots.
- **Project-specific Settings**: This file defines what dbt will do within the scope of your project. The configurations are highly tailored to that particular project, making it version-controlled and tied to the specific project context.

#### Source:
- [dbt_project.yml Reference](https://docs.getdbt.com/reference/project-configs/dbt_project.yml)
   
   
### **Comparison of `dbt_project.yml` vs. `profiles.yml`**

| Feature                  | `dbt_project.yml`                              | `profiles.yml`                              |
|--------------------------|------------------------------------------------|---------------------------------------------|
| **Purpose**               | Project-specific settings and configuration    | Global or environment-specific configuration |
| **Location**              | Inside the project directory                   | Typically in `~/.dbt/` (outside project directory) |
| **Version Control**       | Included in version control (part of the project) | Usually not version-controlled (contains secrets) |
| **Scope**                 | Applies only to the current dbt project         | Can apply to multiple dbt projects (global settings) |
| **Use for Variables**     | Yes, for project-specific variables             | Yes, but mostly for credentials or connection info |
| **Sensitive Information** | No, not intended for secrets                   | Yes, often contains database credentials     |
| **Environment Handling**  | Can define environment-specific variables, but limited to the project | Designed to handle different environments and targets (dev, test, prod, etc.) |


### **Best Practices: Using `dbt_project.yml` and `profiles.yml` Together**

1. **Store Credentials in `profiles.yml`**:
   - **Example**: If you're working with **DuckDB**, the path to your DuckDB file (e.g., `dev.db`, `prod.db`) and other environment-specific information should go into `profiles.yml`.
   - **Security**: Since `profiles.yml` is often kept outside version control, it's a safe place to store sensitive credentials like database usernames, passwords, and connection strings.

2. **Store Project Variables in `dbt_project.yml`**:
   - **Example**: You can define variables like project settings, model configurations, or environment-specific logic in `dbt_project.yml`.
   - **Version Control**: This file should be included in version control, ensuring that your dbt project is portable and self-contained.

3. **Use `profiles.yml` for Multiple Environments**:
   - In `profiles.yml`, you can define multiple outputs for different environments (e.g., `dev`, `prod`) and switch between them using `--target`. This allows you to run the same dbt project in different environments with separate configurations.

4. **Reference Variables**:
   - **In SQL Models**: Variables defined in `dbt_project.yml` can be accessed in your models using the `{{ var() }}` function.
   - **In Profiles**: You can reference different profiles using `dbt run --target` to run models with specific credentials or settings from `profiles.yml`.


**Running dbt Commands**:
   - For development:
     ```bash
     dbt run --target dev
     ```
   - For production:
     ```bash
     dbt run --target prod
     ```




In this setup, the sensitive information is safely stored in `profiles.yml` (such as paths or credentials), and the project-specific variables and configurations are stored in `dbt_project.yml`.

5. **Set up the directory structure:**

   Your dbt project should follow this structure:

   ```
   .
   ├── models
   │   ├── staging
   │   │   └── csv_imports.sql  # SQL to define CSV imports
   │   ├── transforms.sql       # SQL transformation models
   |   ├── source.yml  
   |   ├── models.yml
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
   - If the sources CSV files are located in other directory, we can use `external_root:value` key-value pair to store the path, and refer them using '{{ target.external_root }}' in the model yml files. More details about this type of Jinja syntax are available in https://docs.getdbt.com/reference/dbt-jinja-functions/env_var.  

2. **Create staging models:**
   - In the `models/staging` folder, create a model (SQL file) that defines how each CSV should be loaded into DuckDB.

   Example SQL to load a CSV:

   ```sql
   {{ config(materialized='table') }}

   select * 
   from read_csv_auto('data/source.csv');
   ```
   
  If you need to load a file outside the project folder and don't want to show the absolute path, you can use the `external_root` key/value pair in the `profiles.yml` and refer it as 
   
   ```sql
   {{ config(materialized='table') }}

   select * 
   from read_csv_auto('{{target.external_root}}/sales_data.csv')
   ```

3. **Run dbt to load and transform data:**

   Once your staging models are ready, run dbt to load the CSVs into DuckDB:

   ```bash
   dbt run --target test  # To run models on the test database
   dbt run --target prod  # To run models on the production database
   ```
   
   The `dbt run --target dev` command is used to execute `dbt` models in a `development` environment. This command compiles and runs all the models defined in your dbt project, but specifically against the "dev" target as specified in your `profiles.yml` file. The "dev" target typically points to a development database or schema, allowing you to test and validate your data transformations without affecting production data. By using this command, data analysts and engineers can safely develop and iterate on their `dbt` models in a separate environment before promoting changes to production. This approach supports best practices in data engineering by enabling testing and validation in a controlled setting prior to deployment.


4. **Query the data:**

   After running dbt, you can query your data directly using `DuckDB` or through `dbt` models.

   Example:

   ```sql
   select * from staging.source_csv;
   ```

5. **Removing staging tables or views**


Best Practices for Removing Staging Tables

    1. **Use Ephemeral Models**: Set staging models as ephemeral in production to avoid creating unnecessary tables.
    2. **Use `on-run-end` Hooks**: Automate the cleanup of staging tables after dbt runs by using hooks in `dbt_project.yml`.
     4. **Manual Cleanup with dbt Operations**: Use dbt macros and `run-operation` commands to manually drop staging tables.

Example:

```bash
dbt run-operation drop_staging_objects --target prod
```


More instructions can be found from the documents : https://github.com/mehd-io/dbt-duckdb-tutorial/tree/main and https://www.youtube.com/watch?v=asxGh2TrNyI.




