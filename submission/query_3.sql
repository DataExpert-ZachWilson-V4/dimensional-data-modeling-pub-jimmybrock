CREATE OR REPLACE TABLE actors_history_scd (
    actor   VARCHAR, -- actor name
    quality_class VARCHAR, -- text rating
    is_active BOOLEAN, -- actor active in current year
    start_date INTEGER, -- begin is_active year
    end_date INTEGER, -- end is_active year 
    current_year INTEGER -- year record pertains to
) 
WITH
(
    format = 'PARQUET',
    partitioning = ARRAY['current_year']
)