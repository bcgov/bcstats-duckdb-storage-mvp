<!-- 
Add a project state badge

See <https://github.com/BCDevExchange/Our-Project-Docs/blob/master/discussion/projectstates.md> 
If you have bcgovr installed and you use RStudio, click the 'Insert BCDevex Badge' Addin.
-->

bcstats-duckdb
============================

## Proposal: Testing DuckDB as a Data Storage Solution for BC Stats Analytics Teams
BC Stats is currently challenged by the fragmentation of large, tabular datasets of various formats and across numerous locations within the branch. This scattered data environment complicates our analytical processes, leading to inefficiencies, reproducibility and repeatability challenges and increased costs. Traditional solutions like Oracle databases and MS SQL Server have been considered for centralizing our data, but they come with significant drawbacks, including high start-up costs, complexity, and resource intensity. 
To address these issues, the Population and Demography and Data Science Partnership teams are exploring leveraging DuckDB, a lightweight, open-source SQL-based database system as a centralized data storage solution. DuckDB is a modern, embedded analytics database designed for efficient processing and querying of gigabytes of data from various sources. Unlike traditional client-server databases, DuckDB operates within the same process as your application, eliminating network overhead and simplifying deployment.
### Objectives
The primary objective of this proposal is to demonstrate the capabilities of DuckDB as a viable alternative to traditional, more costly transactional data management systems. We intend to initiate a small-scale project that will serve as a proof of concept, showcasing DuckDB's efficiency, cost-effectiveness, and ability to handle large, varied datasets. This project will aim to:
1. Centralize a subset of our large population and demography datasets within DuckDB.
2. Simplify the process of setting up, linking, querying and analyzing structured, tabular data from multiple sources.
3. Evaluate DuckDB’s performance in handling complex analytical queries and its integration with existing tools (in comparison to MS SQL server, BC Stats’ current platform).
### Rationale
DuckDB offers several advantages over other transaction database solutions for our use case:
•	Cost-Effectiveness: DuckDB is free and open-source, eliminating licensing costs associated with standalone database servers. DuckDB can be hosted on a local or network drive, has no software dependencies and does not require a server.
•	Performance: Despite being lightweight, DuckDB is feature-rich and optimized for fast analytical queries and can efficiently handle large datasets (e.g., hundreds of gigabyte-scale).
•	Ease of Integration: DuckDB is designed to be easily embedded within various programming environments, making it highly compatible with our existing workflows.
•	Flexibility: DuckDB supports multiple data formats natively, reducing the need for extensive data transformation and preprocessing.
Limitations: DuckDB is not designed for frequent CRUD (create, read, update, and delete) operations, and only one user can write to the DuckDB database at a time (but note many users can read and query simultaneously).  BC Stats is primarily a data analytics shop and our data is not frequently updated so these limitations might be easily mitigated.
### Proof of Concept
To validate DuckDB's capabilities, we propose a small project involving the centralization of a representative sample of our data sets. This project will involve:
•	Migrating a subset of large, multi-format datasets into DuckDB. We plan to test the health client file which supports population estimates data, in a temporary OneDrive folder (we can investigate more appropriate and dedicated server/LAN space for these tables as a next step if success is found with this pilot). 
•	Running a series of analytical queries to benchmark performance against MS SQL Server. The queries will be tested using DBeaver, R, and Python applications.
•	Resourcing and documentation of application set-up, compute speed and resource consumption use/testing will be completed by the Population and Demography and Data Science Partnership teams. 
•	Expertise from the BC Data Service will also be consulted, with a request for a set-up and documentation review. 
### Expected Outcomes
We expect to demonstrate whether DuckDB can provide a streamlined, cost-effective, and performant solution for centralized data storage for analytics. The outcome will guide future decisions on data management across the branch. Results from this proof of concept will be presented October-November 2024 to the Branch or Division.


### Usage

Describe data being used (links), and how to run. Eg:

There are four core scripts that are required for the analysis, they need to be run in order:

-   01\_clean.R
-   02\_analysis.R
-   03\_visualize.R
-   04\_output.R

#### Example

This is a basic example which shows you how to solve a common problem:

``` r
## basic example code
```

### Project Status



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
