## Setting up & testing a DuckDB database using R

A minimal viable product (MVP) prototype of setting up and testing performance of a DuckDB database using R.

### Data

This project uses large data tables as test to evaluate the performance of using a DuckDB database.

### Usage

#### DuckDB SQL

A script for loading CSV files into a DuckDB database using native DuckDB SQL queries:

  * `01_load_duckdb_cli.sql`
  
#### `dbplyr` and DuckDB 

A script for loading CSV files into a DuckDB database using the `dbplyr` and `duckdb` R packages:

  * `01_load_duckdb.R`
  
#### `dbplyr`, `arrow`, and MS SQL Server

For comparison, a script that loads CSV files into MS SQL Server using the `dbplyr`, `arrow`, and `nanoarrow` R packages:

  * `01_load_sql-server.R`
  * A custom function `load_csv_save_db()` is included in `utils/utilis.r`
  
#### `dbplyr` and SQLite

SQLite is included as a lightweight alternative. A script for loading CSV files into an SQLite database using the `dbplyr` R package:

  * `01_load-sqlite.R`
  * A custom function `load_csv_save_db()` is included in `utils/utilis.r`

#### Database Performance Tests

Two scripts are provided to test and compare read performance across DuckDB, MS SQL Server, and SQLite:

  * `03_analysis.R`
  * `05_comparison_dplyr_duckdb.R`
  
#### SQL Test Queries

A SQL script to test specific operations on the population estimates dataset:

  * `test-sql.sql`
