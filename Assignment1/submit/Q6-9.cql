consistency ONE;
consistency;
----9) For a given data point find the closest stations in the north.
select station_name, latitude, distance, time from time_table where line_name='Hutt_Vale_Line' and service_no=1 and latitude > -41.19 LIMIT 1;
