--properly modelled scd table with start and end season 
--we are goinf to teack-	scoring_class scoring_class and is_active boolean,
--current_season as date partition column
--see how player retired and came back and his record change overtime-
--create query that looks all of history and create 1 scd record from all of history
--then take take below table and create on top of it incrementally
create table player_scd(
		player_name text,
		scoring_class scoring_class,
		is_active boolean,
		current_season Integer,
		start_season integer,
		end_season integer,
		primary key (player_name,current_season)

)

-- select player_name,scoring_class, is_active
-- from players
-- where current_season =1996
--how to create above table with no filter
--we wanna see the player is how long how many seasons in current dimention eg. scoring class=bad
--by looking at what was the dimention before thats how we can see howthis work
with previous as(
select 
player_name,
scoring_class, 
current_season,
is_active,
lag(scoring_class,1) over (partition by player_name order by current_season) as previous_scoring_class
from players
),
--create indicator if it changed
indicator as(
select *, case when scoring_class <> previous_scoring_class 
               then 1 else 0 end as scoring_class_change_indicator
from previous
),

with_streaks as (
select *,
    sum(scoring_class_change_indicator) over (partition by player_name order by current_season) as streak_identifier
from indicator)

select player_name,
		streak_identifier,
		is_active,
		scoring_class,
		MIN(current_season) as start_season,
		MAX(current_season) as end_season
from with_streaks
group by player_name,
		streak_identifier,
		is_active,
		scoring_class
order by player_name
------------------------


with last_season_scd as(
select * from player_scd
where current_season=2021
and end_season=2021
),
historical_scd as(
select * from player_scd
where current_season=2021
and end_season<2021
),
this_season_data as(
select * from player_scd
where current_season=2022
)

select * from last_season_scd