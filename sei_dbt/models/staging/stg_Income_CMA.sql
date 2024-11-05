-- Load raw data from the CSV file

WITH loaded_data AS (

    -- Load CSV and specify "REF DATE" as VARCHAR
    SELECT *
    FROM read_csv_auto(
        '{{ target.external_root }}/Income_CMA.csv', 
        columns={'REF_DATE': 'VARCHAR'}
    )

),

transformed_data AS (

    -- Replace spaces with underscores in "REF DATE" column
    SELECT 
        REPLACE("Census family type", ' ', '_') AS Census_family_type,
        REPLACE("Family type composition", ' ', '_') AS Family_type_composition,
        -- Select other columns as needed
        *
    FROM loaded_data

)

SELECT * FROM transformed_data;

