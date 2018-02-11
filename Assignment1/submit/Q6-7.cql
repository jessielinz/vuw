consistency ONE;
consistency;
----7) Find the data point for a service on a day in a time interval
select line_name, service_no, latitude, speed from data_point where line_name='Hutt_Vale_Line' and service_no=1 and day=20170403 and sequence < '2017-04-03 10:47:51' and sequence >'2017-04-03 10:37:49';
