drop table if exists {{target_table}};

create table {{target_table}} AS (
SELECT 
   pull_date,
   airport_code,
   "departures_onTime" as departures_time, 
   departures_delayed, 
   departures_canceled, 
   "arrivals_onTime" as arrivals_time, 
   arrivals_delayed,
   arrivals_canceled 
 FROM flight_data
);