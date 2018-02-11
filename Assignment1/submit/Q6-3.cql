consistency ONE;
consistency;
----3) Get a service due to be dispatched next.
select line_name, service_no, dept_stat_name, time from dept_stat_time_table where time > 0735 LIMIT 1 ALLOW FILTERING;
