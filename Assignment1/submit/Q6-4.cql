consistency QUORUM;
consistency;
----4) Find an available vehicle at the departure station.
select vehicle_id, type from vehicle where status = 'Wellington' LIMIT 1;
