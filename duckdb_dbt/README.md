## Setting up a DuckDB database using `dbt-duckdb`

This folder is for a variation on loading data into a DuckDB database. These scripts load multiple CSV files into a DuckDB database using `dbt-duckdb`. It helps automate the process of transforming, modeling, and querying data from CSV files in a simple and efficient way. DuckDB is used as the local database engine, and `dbt` orchestrates the loading and transformation process.

### Contents

This folder is entirely self-contained, and requires no scripts/knowledge from the main folder. The main contents include:

1. [macros](macros)
   * This folder includes a `dbt` script for dropping staged objects from the production environment. 

2. [models](models)
   * This folder includes both `sql` and `yaml` files:
       * The `sql` files read CSV files into a DuckDB database with the table name equal to the associated sql name.
       * Sql in the staging folder directly reads in raw data, while `sql` in the models folder manipulates the tables produced in the staging process. 
       * The `schema.yml` file enforces column names, types, and tests to run when creating tables in the database. 
       * The `sources.yml` file contains information about the location and description of raw source files. 

3. `dbt_projects.yml`:
   * This is the main file that tells `dbt` what to do when a run command is issued.

4. `packages.yml`:
   * This contains `dbt` packages that are required to fully run the `dbt` sequence in this project. 

## Requirements

Since this was built using the Python package `dbt-duckdb`, ensure you have the following tools installed:

* Python (>=3.7)
* Python versions of `duckdb` and `dbt`:
    * `pip install dbt-duckdb`
    * `pip install duckdb`

Note that instead of using `pip`, you may also choose to install these into a clean conda environment. 


### New Python environment
To create a new Python environment for **dbt-duckdb**, 

* Ensuring you have **Python 3.11 or later** installed. 

* Use a virtual environment tool like `venv` or `conda` to isolate dependencies for your project. 
  - To use `venv`, navigate to your project directory in the terminal and run `python -m venv venv` to create the environment. 
     * Activate it using `source venv/bin/activate` (Linux/Mac) or `venv\Scripts\activate` (Windows). 
  - To use `conda`, ensure you have Anaconda or Miniconda installed. 
    * If not, download and install Miniconda from Miniconda Downloads.
    * Open your terminal or command prompt and run the following command to create a new environment named dbt_env with Python 3.8 or later:
        `conda create --name dbt_env python=3.11`
    * Activate the Environment: Activate the new environment using:
        `conda activate dbt_env`
  
* Install `dbt-duckdb` and its dependencies by running `python -m pip install --upgrade dbt-core==1.8.6 dbt-common dbt-adapters dbt-duckdb==1.8.3`; or `pip install-r requirments.txt`. `requirments.txt` is located in this folder and has the packages that we need for this `dbt` project.

* Verify the installation with `dbt --version`, and ensure the correct Python interpreter is used in your project settings if working with an IDE. 


### `dbt` packages

This setup keeps your `dbt-duckdb` installation isolated and compatible with your project requirements.

In this case, any further `dbt` commands should always be run from within that active conda environment. 

Certain elements of this project also require `dbt` packages. Install required packages from the `packages.yml` file using this command (from the command line inside your `dbt` conda environment):
* `dbt deps`

You will also need to have a `profiles.yml` file saved in this folder. Copy this example into your local repository, and update the `external_root` and `path` variables to point to the appropriate CAV/database locations:

```yaml
duckdb_dbt:
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

### Usage

To set up the DuckDB database with our raw CSV data, first ensure that all CSVs are in the correct format (UTF-8). This can be done by running the `convert.sh` file from a git bash command line (**not a windows command prompt!**):

`./convert.sh`

Then, to run through the `dbt` pipeline, simply run a single command (this time from a regular Windows command line where all required Python/`dbt` packages are recognized. If using conda to manage your environment, activate your `dbt` environment first):

`dbt run`

This will:
1. Run any `sql` in the `models/staging` folder and produce associated DuckDB tables.
2. Run any `sql` in the `models` folder to produce aggregated tables based on these staged files. 
3. Run `on-run-end` hook which call the clean up macro to remove staging tables. 

Once complete, your DuckDB database will now be populated with the contents of the CSVs. 

Note that this will likely contain multiple new local folders. These folders do not need to be committed to the repository, and have been included in the repository `.gitignore`. 

### Resources

More resources on building database pipelines via `dbt` and `duckdb` can be found here: 

* https://github.com/mehd-io/dbt-duckdb-tutorial/
* https://github.com/duckdb/dbt-duckdb
* https://duckdb.org/docs/api/python/overview.html
* https://github.com/dbt-labs/jaffle_shop_duckdb
* https://learn.getdbt.com/catalog

