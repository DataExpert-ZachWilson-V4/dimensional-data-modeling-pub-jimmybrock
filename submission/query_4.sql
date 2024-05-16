-- Batch backfill query that can populate the entire 
-- actors_history_scd table in a single query
INSERT INTO  actors_history_scd
-- get previous year data so we can compare to current year
WITH lagged AS (
        SELECT
            actor,
            quality_class,
            -- get last years quality class
            LAG(quality_class, 1) OVER (
                    PARTITION BY actor
                    ORDER BY
                        current_year
                ) AS quality_class_last_year,
            is_active,
            -- get last years active status
            LAG(is_active, 1) OVER (
                    PARTITION BY actor
                    ORDER BY
                        current_year
                )
            as is_active_last_year,
            current_year
        FROM
             actors
        WHERE
            current_year <= 2021
    ),
    -- how long has actor been active
    streaked AS (
        SELECT
            *,
            SUM(
                -- compare active status and quality_class for 
                -- changes to see if streaked
                CASE
                    WHEN is_active <> is_active_last_year
                    OR quality_class <> quality_class_last_year 
                        THEN 1
                    ELSE 0
                END
            ) OVER (
                PARTITION BY actor
                ORDER BY
                    current_year
            ) AS tracked_changes
        FROM
            lagged
    )
    -- new records for scd history
SELECT
    actor,
    quality_class,
    is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    2021 as current_year
FROM
    streaked
GROUP BY
    actor,
    is_active,
    quality_class,
    tracked_changes
