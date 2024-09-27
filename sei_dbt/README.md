Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test

1.	A dbt workflow has been established in “dbt”
    a.	The basic setup follows : https://github.com/mehd-io/dbt-duckdb-tutorial/tree/main and https://www.youtube.com/watch?v=asxGh2TrNyI
        i.	This project use the dbt-duckdb adapter for DuckDB. You can install it by doing `pip install dbt-duckdb`. This include dbt, dbt-duckdb adapter and duckdb.
        ii.	Inside the dbt project /dbt,  run `dbt run`
        iii.	It will implement the models in “models” folder and create tables or output CSV files.

2.	A GitHub action has been established in the workflow.yaml file to run the dbt project. This is to ensure the project can be run in a CI/CD pipeline. Not test yet.

3. Now a fin-neighborhood-incomes-2021.csv file has been added to the external_source folder. This is to test the ability to read from a CSV file. After the staging, this table should be stacked with fin-neighborhood-incomes-2016-2020.csv. 

4. The table_98_401_X2021025_English_CSV_data.csv file has been added to the external_source folder. This is to test the ability to read from a CSV file. After the staging, this table should be stacked with table_98_401_X2021006_English_CSV_data_BritishColumbia.csv. 


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
