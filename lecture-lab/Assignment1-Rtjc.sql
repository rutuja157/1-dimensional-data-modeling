drop TYPE film_struct
drop table actors
CREATE TYPE film_struct AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

CREATE TYPE quality_class as ENUM('star','good','average','bad');

CREATE TABLE actors (
    actor_id text PRIMARY KEY,
    actor_name TEXT NOT NULL,
    films film_struct[],  
    quality_class quality_class,
    is_active BOOLEAN NOT NULL
);


-- most recent year
WITH recent_years AS (
    SELECT actorid, MAX(year) AS max_year
    FROM actor_films
    GROUP BY actorid
),
--determined by the average rating of movies of their most recent year-per actor
recent_year_ratings AS (
    SELECT
        af.actorid,
        AVG(af.rating) AS avg_rating
    FROM actor_films af
    JOIN recent_years ry
        ON af.actorid = ry.actorid AND af.year = ry.max_year
    GROUP BY af.actorid
)
INSERT INTO actors (actor_id, actor_name, films, quality_class, is_active)
--represents an actor's performance quality
SELECT
    af.actorid,
    af.actor,
    ARRAY_AGG(ROW(af.film, af.votes, af.rating, af.filmid)::film_struct ORDER BY af.year DESC) AS films,
    -- Join with recent_year_ratings to get a single value per actor
    CASE
        WHEN ryr.avg_rating > 8 THEN 'star'
        WHEN ryr.avg_rating > 7 THEN 'good'
        WHEN ryr.avg_rating > 6 THEN 'average'
        ELSE 'bad'
    END::quality_class AS quality_class,
    -- is_active: has a film in the current year?
    MAX(CASE WHEN af.year = EXTRACT(YEAR FROM CURRENT_DATE) THEN TRUE ELSE FALSE END) AS is_active
FROM actor_films af
JOIN recent_year_ratings ryr ON af.actorid = ryr.actorid
GROUP BY af.actorid, af.actor, ryr.avg_rating;

select *from actors
---
select max(year) from actor_films
---------
-- 2. **Cumulative table generation query:** Write a query that populates the actors table one year at a time.
WITH recent_years AS (
    SELECT actorid, MAX(year) AS max_year
    FROM actor_films
    GROUP BY actorid
),
--determined by the average rating of movies of their most recent year-per actor
recent_year_ratings AS (
    SELECT
        af.actorid,
        AVG(af.rating) AS avg_rating
    FROM actor_films af
    JOIN recent_years ry
        ON af.actorid = ry.actorid AND af.year = ry.max_year
    GROUP BY af.actorid
)
INSERT INTO actors (actor_id, actor_name, films, quality_class, is_active)
--represents an actor's performance quality
SELECT
    af.actorid,
    af.actor,
    ARRAY_AGG(ROW(af.film, af.votes, af.rating, af.filmid)::film_struct ORDER BY af.year DESC) AS films,
    -- Join with recent_year_ratings to get a single value per actor
    CASE
        WHEN ryr.avg_rating > 8 THEN 'star'
        WHEN ryr.avg_rating > 7 THEN 'good'
        WHEN ryr.avg_rating > 6 THEN 'average'
        ELSE 'bad'
    END::quality_class AS quality_class,
    -- is_active: has a film in the current year?
    MAX(CASE WHEN af.year = EXTRACT(YEAR FROM CURRENT_DATE) THEN TRUE ELSE FALSE END) AS is_active
FROM actor_films af
JOIN recent_year_ratings ryr ON af.actorid = ryr.actorid
where af.year = 1970
GROUP BY af.actorid, af.actor, ryr.avg_rating;

-------
-- 3. **DDL for actors_history_scd table:** Create a DDL for an actors_history_scd table with the following features:
--     - Implements type 2 dimension modeling (i.e., includes start_date and end_date fields).
--     - Tracks quality_class and is_active status for each actor in the actors table.
drop table actors_history_scd;
CREATE TABLE actors_history_scd (
    actor_id      TEXT NOT NULL,
    actor_name    TEXT    NOT NULL,
    films         film_struct[],         
    quality_class quality_class NOT NULL, 
    is_active     BOOLEAN NOT NULL,

    start_date    integer NOT NULL,        -- SCD type 2: when this record became active
    end_date      integer,                 -- SCD type 2: when this record was superseded; NULL = currently active

    PRIMARY KEY (actor_id,start_date)
);



-- 4. **Backfill query for `actors_history_scd`:** Write a "backfill" query that can populate 
-- the entire `actors_history_scd` table in a single query.
WITH avg_ratings_by_year AS (
SELECT
    actorid AS actor_id,
    actor AS actor_name,
    year,
    AVG(rating) AS avg_rating
  FROM actor_films
  GROUP BY actorid, actor, year
),
films_by_year AS (
  SELECT
    actorid AS actor_id,
    actor AS actor_name,
    year,
    ARRAY_AGG(
      ROW(film, votes, rating, filmid)::film_struct
      ORDER BY filmid
    ) AS films
  FROM actor_films
  GROUP BY actorid, actor, year
),
actors_by_year AS (
  SELECT
    a.actor_id,
    a.actor_name,
    a.year,
    f.films,
    CASE
      WHEN a.avg_rating > 8 THEN 'star'
      WHEN a.avg_rating > 7 THEN 'good'
      WHEN a.avg_rating > 6 THEN 'average'
      ELSE 'bad'
    END::quality_class AS quality_class,
    TRUE AS is_active
  FROM avg_ratings_by_year a
  JOIN films_by_year f
    ON a.actor_id = f.actor_id
    AND a.year = f.year
)
, change_flags AS (
  SELECT *,
    LAG(quality_class) OVER (PARTITION BY actor_id ORDER BY year) AS prev_quality_class,
    LAG(is_active) OVER (PARTITION BY actor_id ORDER BY year) AS prev_is_active
  FROM actors_by_year
),
c_indicator as(
select *, CASE
        WHEN quality_class != prev_quality_class
          OR is_active != prev_is_active
          OR prev_quality_class IS NULL
        THEN 1 ELSE 0 END as change_indicator
from change_flags
),
grouped_changes as(
SELECT *,
    SUM(change_indicator) over (partition by actor_id order by year) as scd_group
from c_indicator
)
--Find Start/End Dates for Each SCD Period
, scd_periods AS (
  SELECT
    actor_id,
	MIN(actor_name) AS actor_name,
	scd_group,
	films,
	quality_class,
	is_active,
	-- FIRST_VALUE(films) OVER (PARTITION BY actor_id, scd_group ORDER BY year) AS films,
 --    FIRST_VALUE(quality_class) OVER (PARTITION BY actor_id, scd_group ORDER BY year) AS quality_class,
 --    FIRST_VALUE(is_active) OVER (PARTITION BY actor_id, scd_group ORDER BY year) AS is_active,
    MIN(year) AS start_date,
    MAX(year) AS end_date
    
  FROM grouped_changes
  GROUP BY actor_id,actor_name,scd_group,films,quality_class,is_active
  order by actor_id,actor_name
)


INSERT INTO actors_history_scd (
  actor_id, actor_name, films, quality_class, is_active, start_date, end_date
)
select actor_id, actor_name, films, quality_class, is_active, start_date, end_date from scd_periods

select * from actors_history_scd


-- 5. **Query to retrieve current active actors:** Write a query that retrieves all currently active actors from the `actors_history_scd` table.
SELECT
    a.actor_id,
    a.actor_name,
    a.films,
    a.quality_class,
    a.is_active,
    CURRENT_DATE   -- or effective date from actors table if available
    
FROM actors a
LEFT JOIN actors_history_scd scd
  ON a.actor_id = scd.actor_id
  AND scd.end_date IS NULL
WHERE  a.is_active = 'true' and
  scd.actor_id IS NULL -- new actor
  OR
  (
    scd.quality_class <> a.quality_class OR
    scd.is_active <> a.is_active
  )
  ;