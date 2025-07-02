-- SELECT * FROM public.player_seasons where player_name = 'A.C. Green'
-- create TYPE season_stats as(
-- 						season INTEGER,	
-- 						gp INTEGER,
-- 						pts REAL,
-- 						reb REAL,
-- 						ast REAL
-- 						)

-- create type scoring_class as ENUM ('star','good','average','bad');

-- DROP table players;
-- CREATE TABLE players(
-- 					player_name TEXT,
-- 					height TEXT,
-- 					college TEXT,
-- 					country TEXT,
-- 					draft_year TEXT,
-- 					draft_round TEXT,
-- 					draft_number TEXT,
-- 					season_stats season_stats[],
-- 					scoring_class scoring_class,
-- 					year_since_last_season Integer,
-- 					current_season INTEGER,
-- 					is_active BOOLEAN,
-- 					PRIMARY KEY(player_name,current_season)
-- )


----create pipeline insert season data one by one from 1996-2001  -only did 1997
Insert into players
WITH yesterday as(
select * from players
where current_season = 2000
),
today as (
select * from player_seasons
where season =2001
)
select 
coalesce (t.player_name,y.player_name) as player_name,
coalesce (t.height,y.height) as height,
coalesce (t.college,y.college) as college,
coalesce (t.country,y.country) as country,
coalesce (t.draft_year,y.draft_year) as draft_year,
coalesce (t.draft_round,y.draft_round) as draft_round,
coalesce (t.draft_number,y.draft_number) as draft_number,
case when y.season_stats is null then ARRAY[row(
												t.season,
												t.gp,
												t.pts,
												t.reb,
												t.ast
											)::season_stats]
	                                when t.season is not null then 
									y.season_stats || ARRAY[row(
												t.season,
												t.gp,
												t.pts,
												t.reb,
												t.ast
											)::season_stats]
									else y.season_stats
END as season_stats,
case 
	when t.season is not null then 
		case when t.pts > 20 then 'star'
		 	when t.pts > 15 then 'good'
		 	when t.pts >10 then 'average'
		 	else 'bad'
    	end::scoring_class
	else y.scoring_class
end as scoring_class,

case when t.season is not null then 0
     else y.year_since_last_season + 1
end as years_since_last_season,

coalesce (t.season,y.current_season+1) as current_season
from today t FULL OUTER JOIN yesterday y on t.player_name = y.player_name

select * from players where current_season = 2001 and player_name='Michael Jordan'


--this below we get all sorted data , when you use cumulative table design, 
-- it doent mess with sorted data when you cumulate do and then do join and then unnest then its all already sorted
WITH
	UNNESTED_CTE AS (
		SELECT
			PLAYER_NAME,
			UNNEST(SEASON_STATS)::SEASON_STATS AS SEASON_STATS
		FROM
			PLAYERS
		WHERE
			CURRENT_SEASON = 1997
			-- and player_name = 'A.C. Green'
	)
SELECT
	PLAYER_NAME,
	(SEASON_STATS::SEASON_STATS).*
FROM
	UNNESTED_CTE

-- which player has the biggest improbement from their 1st season to their most recet season

-- select player_name,
-- season_stats[1] as first_season,
-- season_stats[cardinality(season_stats)] as latest_season
-- from players
-- where current_season = 2001

-- select player_name,
-- (season_stats[1]::season_stats).pts as first_season,
-- (season_stats[cardinality(season_stats)]::season_stats).pts as latest_season
-- from players
-- where current_season = 2001

-- select player_name,
-- (season_stats[cardinality(season_stats)]::season_stats).pts /
-- case when (season_stats[1]::season_stats).pts=0 then 1 else (season_stats[1]::season_stats).pts end
-- from players
-- where current_season = 2001
-- order by 2 desc