SELECT 
   pull_date,
   airport_code,
   "departures_onTime",
    departures_delayed,
     departures_canceled,
      "arrivals_onTime",
       arrivals_delayed,
       arrivals_canceled 
 FROM 
      {{source_table}}