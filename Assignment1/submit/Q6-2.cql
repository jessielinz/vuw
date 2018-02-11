consistency ONE;
consistency;
----2) Publish a time table for passengers.
select line_name, service_no, station_name, time from time_table where line_name = 'Hutt_Vale_Line' ALLOW FILTERING;
