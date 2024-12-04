#!/bin/bash

# Run dbt models
dbt run --target prod

# Run the macro to drop staging tables/views
dbt run-operation drop_staging_objects --target prod
