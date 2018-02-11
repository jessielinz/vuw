consistency QUORUM;
consistency;
----5) Find an available driver having the needed skill at the departure station.
select driver_name, mobile from driver where skills CONTAINS 'Kiwi Rail' and current_position = 'Wellington' LIMIT 1 ALLOW FILTERING;
