-- CUMULATIVE TABLE COMPUTATION QUERY
INSERT INTO actors
WITH
    -- get last years data
  last_year AS (
    SELECT
      *
    FROM
      actors
    WHERE
      current_year = 1913
  ),
  -- get the current year data
  this_year AS (
    SELECT
      a.actor,  -- actor name
      a.actor_id,   -- actors unique identifier
      array_agg(row(
        a.film,     -- film name
        a.votes,    -- number of votes for the film
        a.rating,   -- rating for the film 
        a.film_id   -- unique film Identifier
        )) 
      AS films,
      avg(a.rating) AS avg_rating,  -- average rating for film
      avg(a.votes) AS avg_votes,    -- average voes for film
      a.YEAR    -- year film came out
    FROM
      bootcamp.actor_films a
    WHERE
      a.YEAR = 1914
      -- partition by actor/year
    GROUP BY
      a.actor,
      a.actor_id,
      a.YEAR
  )
SELECT
    -- get non null values if possible
  coalesce(ls.actor, ts.actor) AS actor,
  coalesce(ls.actor_id, ts.actor_id) AS actor_id,
  -- handle null values
  CASE
    WHEN ts.year IS NULL THEN ls.films
    WHEN ts.year IS NOT NULL
    AND ls.films IS NULL THEN ts.films
    WHEN ts.year IS NOT NULL
    AND ls.films IS NOT NULL THEN ts.films || ls.films
  END AS films,
  coalesce(
    -- assign avg ratings
    CASE
      WHEN avg_rating > 8.0 THEN 'star'
      WHEN avg_rating > 7.0
      AND avg_rating <= 8.0 THEN 'good'
      WHEN avg_rating > 6.0
      AND avg_rating <= 7.0 THEN 'average'
      WHEN avg_rating <= 6 THEN 'bad'
    END,
    ls.quality_class
  ) AS quality_class,
  ts.actor IS NOT NULL AS is_active,
  COALESCE(ts.year, ls.current_year + 1) current_year
FROM
  last_year ls
  FULL OUTER JOIN this_year ts ON ls.actor_id = ts.actor_id  
