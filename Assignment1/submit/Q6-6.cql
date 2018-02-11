consistency ONE;
consistency;
----6) Find the last data point for a service on a day
select line_name, service_no, latitude, speed from data_point where line_name='Hutt_Vale_Line' and service_no=1 and day=20170403;
