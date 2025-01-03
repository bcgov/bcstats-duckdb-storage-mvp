<!-- badges: start -->
[![Lifecycle:Experimental](https://img.shields.io/badge/Lifecycle-Experimental-339999)](https://github.com/bcgov/repomountie/blob/master/doc/lifecycle-badges.md)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/license/apache-2-0/)
<!-- badges: end -->

## bcstats-duckdb-storage-mvp

Testing DuckDB as a data storage & query solution.

Code to load multiple (large) CSV files into a [DuckDB](https://duckdb.org) database using [dbt-duckdb](https://github.com/duckdb/dbt-duckdb). DuckDB is used as the local database engine, and `dbt` orchestrates the loading and transformation process.

### Usage

A working MVP for loading multiple CSV files into a DuckDB database using `dbt-duckdb` is located in the `/duckdb_dbt` folder. See `/duckdb_dbt/README.md` for details. 

A similar `dbt` project with MS SQL server is located in the `/mssql_dbt` folder. 

Some initial prototyping and performance testing was implemented using R&mdash;scripts and documentation of this approach are included in the `/r-implementation` folder. See `/r-implementation/README.md` for details. 



### Project Status
An MVP project currently in active development.

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
