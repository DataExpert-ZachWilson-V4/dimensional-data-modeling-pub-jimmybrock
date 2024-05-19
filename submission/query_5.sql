-- incremental query that can populate a single years 
-- worth of the actors_history_scd table by combining 
-- the previous year's SCD data with the new incoming data from the actors table for this year.

insert into actors_history_scd

-- last year data for a single year
WITH
  last_year_scd AS (
    SELECT
      *
    FROM
      actors_history_scd
    WHERE
      current_year = 1943
  ),
  -- this year data for a single year
  current_year_scd AS (
    SELECT
      *
    FROM
      actors
    WHERE
      current_year = 1944
  ), 
  -- combine two datasets 
  combined as 
    (
    SELECT
    -- get not null values if possible
    coalesce(ls.actor, cs.actor) as actor,
    coalesce(ls.start_date, cs.current_year) as start_date,
    coalesce(ls.end_date, cs.current_year) as end_date,
    -- check if data change between years for 
    -- active status and quality_change
    CASE
      WHEN (ls.is_active <> cs.is_active) or 
          (ls.quality_class <> cs.quality_class) THEN 1
      WHEN (ls.is_active = cs.is_active) and 
          (ls.quality_class = cs.quality_class) THEN 0
    END AS did_change, 
    ls.is_active as is_active_last_year, 
    cs.is_active as is_active_this_year,
    ls.quality_class as quality_class_last_year, 
    cs.quality_class as quality_class_this_year,
    1944 as current_year
FROM
  -- full outer join to get all change records
  last_year_scd ls
  FULL OUTER JOIN current_year_scd cs ON cs.actor = ls.actor
  AND cs.current_year = ls.end_date + 1
  ), changes as
  (
  select actor,current_year,
    case when did_change = 0  
        then ARRAY[cast(row(quality_class_last_year,
                      is_active_last_year,
                      start_date,
                      end_date+1)
                      as ROW(
                            quality_class varchar,
                            is_active boolean,
                            start_date integer,
                            end_date integer
                  ))]
          when did_change = 1
          then ARRAY[cast(row(quality_class_last_year,
                      is_active_last_year,
                      start_date,
                      end_date) as 
                      ROW(
                            quality_class varchar,
                            is_active boolean,
                            start_date integer,
                            end_date integer
                      )),
                      cast(
                      row(quality_class_this_year,
                      is_active_this_year,
                      current_year,
                      current_year)
                      as ROW(
                            quality_class varchar,
                            is_active boolean,
                            start_date integer,
                            end_date integer
                      ))]
        when did_change is null
        then ARRAY[cast(row(
        coalesce(quality_class_last_year,quality_class_this_year),
                      coalesce(is_active_last_year,is_active_this_year),
                      start_date,
                      end_date)
                      as ROW(
                            quality_class varchar,
                            is_active boolean,
                            start_date integer,
                            end_date integer
                  ))]
    end as change_array
  from combined
  )
  -- unnest changes and join
  select 
  actor,
  arr.quality_class,
  arr.is_active,
  arr.start_date,
  arr.end_date,
  current_year
  from changes 
  cross join unnest(change_array) as arr
