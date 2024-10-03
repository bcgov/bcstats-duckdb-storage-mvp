---
## Purpose

This folder is for a variation on loading data into a duckDB warehouse. While the main project folder contains various R scripts for testing duckDB and its capabilities, this sub-project is created to load multiple CSV files into a DuckDB database using dbt-duckdb (Data Build Tool). It helps automate the process of transforming, modeling, and querying data from CSV files in a simple and efficient way. DuckDB is used as the local database engine, and dbt orchestrates the loading and transformation process.

## Contents

This folder is entirely self-contained, and requires no scripts/knowledge from the main folder. The main contents include:

1. [macros](macros)
* This folder includes a dbt script for dropping staged objects from the production environment. 

2. [models](models)
* This folder includes both `sql` and `yaml` files:
    * The `sql` files read CSV files into a duckDB database with the table name equal to the associated sql name
    * Sql in the staging folder directly reads in raw data, while sql in the models folder manipulates the tables produced in the staging process. 
    * The `schema.yml` file enforces column names, types, and tests to run when creating tables in the database. 
    * The `sources.yml` file contains information about the location and description of raw source files. 

3. `dbt_projects.yml`:
* This is the main file that tells dbt what to do when a run command is issued.

4. `packages.yml`:
* This contains dbt packages that are required to fully run the dbt sequence in this project. 


## Requirements

Since this was built using the Python package `dbt-duckdb`, ensure you have the following tools installed:

* Python (>=3.7)
* Python versions of duckdb and `dbt`:
    * `pip install dbt-duckdb`
    * `pip install duckdb`

Certain elements of this project also require `dbt` packages. Install required packages from the `packages.yml` file using this command (from the command line):
* `dbt deps`


You will also need to have a `profiles.yml` file saved in this folder. Copy this example into your local repository, and update the `external_root` and `path` variables to point to the appropriate csv/database locations:

```yaml
sei_dbt:
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

## Usage

To set up the duckDB database with our raw csv data, first ensure that all csvs are in the correct format (UTF-8). This can be done by running the `sei_dbt.sh` file from a git bash command line (**not a windows command prompt!**):

`./sei_dbt.sh`

Then, to run through the dbt pipeline, simply run a single command:

`dbt run`

This will:
1. Run any sql in the `models/staging` folder and produce associated duckdb tables.
2. Run any sql in the `models` folder to produce aggregated tables based on these staged files. 
3. Run `on-run-end` hook which call the clean up macro to remove staging tables. 

Once complete, your duckdb database will now be populated with the contents of the csvs. 

Note that this will likely contain multiple new local folders. These folders do not need to be committed to the repository, and have been included in the `.gitignore`. 

## Resources

More resources on building database pipelines via `dbt` and `duckdb` can be found here: 

* https://github.com/mehd-io/dbt-duckdb-tutorial/
* https://github.com/duckdb/dbt-duckdb
* https://duckdb.org/docs/api/python/overview.html


Copyright 2024 Province of British Columbia

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at 

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
