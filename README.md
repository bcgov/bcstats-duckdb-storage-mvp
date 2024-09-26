<!-- 
Add a project state badge

See <https://github.com/BCDevExchange/Our-Project-Docs/blob/master/discussion/projectstates.md> 
If you have bcgovr installed and you use RStudio, click the 'Insert BCDevex Badge' Addin.
-->

bcstats-duckdb-storage-mvp
============================

## Testing DuckDB as a Data Storage Solution for BC Stats Analytics Teams

### Objectives
The primary objective of this proposal is to demonstrate the capabilities of DuckDB as a viable alternative to traditional, more costly transactional data management systems. We intend to initiate a small-scale project that will serve as a proof of concept, showcasing DuckDB's efficiency, cost-effectiveness, and ability to handle large, varied datasets. This project will aim to:
1. Centralize a subset of our large population and demography datasets within DuckDB.
2. Simplify the process of setting up, linking, querying and analyzing structured, tabular data from multiple sources.
3. Evaluate DuckDB’s performance in handling complex analytical queries and its integration with existing tools. 

### Rationale
DuckDB offers several advantages over other transaction database solutions for our use case:
1. Cost-Effectiveness: DuckDB is free and open-source, eliminating licensing costs associated with standalone database servers. DuckDB can be hosted on a local or network drive, has no software dependencies and does not require a server.
2.	Performance: Despite being lightweight, DuckDB is feature-rich and optimized for fast analytical queries and can efficiently handle large datasets (e.g., hundreds of gigabyte-scale).
3.	Ease of Integration: DuckDB is designed to be easily embedded within various programming environments, making it highly compatible with our existing workflows.
4.	Flexibility: DuckDB supports multiple data formats natively, reducing the need for extensive data transformation and preprocessing.
Limitations: DuckDB is not designed for frequent CRUD (create, read, update, and delete) operations, and only one user can write to the DuckDB database at a time (but note many users can read and query simultaneously).  BC Stats is primarily a data analytics shop and our data is not frequently updated so these limitations might be easily mitigated.


## Overview

This project is designed to load multiple CSV files into a DuckDB database using **dbt** (Data Build Tool). It helps automate the process of transforming, modeling, and querying data from CSV files in a simple and efficient way. DuckDB is used as the local database engine, and dbt orchestrates the loading and transformation process.

## Features

- **Automated CSV Loading:** Easily load multiple CSV files into DuckDB.
- **Data Transformation:** Use dbt to perform SQL-based data transformations on the loaded CSVs.
- **Lightweight Database:** Leverages DuckDB, which is optimized for efficient data processing on local files.
- **Flexible:** Customize dbt models to suit your business logic and transformation requirements.

---

## Requirements

Before starting, ensure you have the following tools installed:

1. **Python** (>= 3.7)
2. **dbt** (>= 0.19.0)
3. **DuckDB** (Python package or standalone)
4. **CSV Files:** Ensure you have your source CSV files ready.

To install the required Python packages:

```bash
pip install dbt-duckdb duckdb
```

---

## Setup

Follow these steps to set up and configure the project:

1. **Clone the repository:**

   ```bash
   git clone <your-repository-url>
   cd <project-folder>
   ```

2. **Install dependencies:**

   Ensure you have dbt and DuckDB installed. Then, install the dependencies required for dbt to work with DuckDB:

   ```bash
   pip install -r requirements.txt
   ```

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

---

## Project Status

This project is currently in active development. The core functionality of loading CSV files into DuckDB using dbt is complete. Future improvements may include:

- Support for additional file formats (e.g., Parquet, JSON).
- Enhanced data validation and error handling.
- Automating more transformations and data processing workflows.


### Getting Help or Reporting an Issue

To report bugs/issues/feature requests, please file an [issue](https://github.com/bcgov/bcstats-duckdb/issues/).

### How to Contribute

If you would like to contribute, please see our [CONTRIBUTING](CONTRIBUTING.md) guidelines.

Please note that this project is released with a [Contributor Code of Conduct](CODE_OF_CONDUCT.md). By participating in this project you agree to abide by its terms.

### License

```
Copyright 2024 Province of British Columbia

Licensed under the Apache License, Version 2.0 (the &quot;License&quot;);
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
```
---
*This project was created using the [bcgovr](https://github.com/bcgov/bcgovr) package.* 
