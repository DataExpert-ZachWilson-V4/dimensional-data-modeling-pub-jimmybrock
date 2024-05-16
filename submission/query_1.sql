CREATE OR REPLACE TABLE actors (
        -- actor name
        actor       varchar,
        -- actor unique identifer 
        actor_id    varchar NOT NULL,
        films -- film attributes
            ARRAY(ROW(
                film    VARCHAR, -- film name
                votes   INTEGER, -- number of film votes
                rating  DOUBLE,  -- film rating
                film_id VARCHAR  -- film unique identifier
            )
        ),
      -- Category rating based on avg rating for films in the most recent year
      quality_class    VARCHAR,
      -- actor is currently active in making films
      is_active        BOOLEAN,
      -- current year of film
      current_year     INTEGER
)
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY ['current_year']
    )