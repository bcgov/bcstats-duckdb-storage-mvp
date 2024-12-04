-- Load raw data from the CSV file

WITH loaded_data AS (

    -- Load CSV and specify "REF DATE" as VARCHAR
    SELECT *
    FROM read_csv_auto(
        '{{ target.external_root }}/Income_CMA.csv', 
        types={'REF_DATE': 'VARCHAR'},
        --all_varchar = true
        ignore_errors=true
    )

),

transformed_data AS (

    -- Replace spaces with underscores in "some column" column name
    SELECT 
        REF_DATE,
        GEO,
        DGUID,
        "Census family type" AS Census_family_type,
        "Family type composition" AS Family_type_composition,
        -- Select other columns as needed
        Statistics,
        VALUE

    FROM loaded_data

)

SELECT * FROM transformed_data

