/* ==== Q4 ==== */

---Create a keyspace
CREATE KEYSPACE IF NOT EXISTS tranzmetro WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':3};

USE tranzmetro;

---1) driver table
CREATE TABLE driver (
    driver_name text,
    password text,
    mobile text,
    current_position text,
    skills set<text>,
    PRIMARY KEY (driver_name));

---Create index for current_position column of driver table
CREATE INDEX driver_current_position_idx ON tranzmetro.driver (current_position);

---2) vehicle table
CREATE TABLE vehicle (
    vehicle_id text,
    status text,
    type text,
    PRIMARY KEY (vehicle_id));

---Create index for status column of vehicle table
CREATE INDEX vehicle_status_idx ON tranzmetro.vehicle (status);

---3) vehicle distance table
CREATE TABLE vehicle_distance (
    vehicle_id text,
    date int,
    daily_distance double,
    total_distance double static,
    PRIMARY KEY (vehicle_id, date));

---4) time table
CREATE TABLE time_table (
    line_name text,
    service_no int,
    latitude double,
    station_name text,
    time int,
    distance double,
    PRIMARY KEY ((line_name, service_no),latitude,time));

----5) departure station time table
CREATE TABLE dept_stat_time_table (
    line_name text,
    service_no int,
    dept_stat_name text,
    time int,
    PRIMARY KEY ((line_name,service_no), time))
WITH CLUSTERING ORDER BY (time DESC);

----6) data point table
CREATE TABLE data_point (
    line_name text,
    service_no int,
    day int,
    sequence timestamp,
    latitude double,
    longitude double,
    speed double,
    PRIMARY KEY ((line_name, service_no, day), sequence))
WITH CLUSTERING ORDER BY (sequence DESC)
 AND COMPACTION = {'class':'LeveledCompactionStrategy'};

---7) driver attendance table
CREATE TABLE driver_attendance (
    driver_name text,
    month int,
    monthly_days counter,
    PRIMARY KEY (driver_name, month));

/*==== Q5 ==== */

---Create new data for driver table
consistency;
Insert into driver(driver_name,current_position,mobile,password,skills) values('Jane','Waikanae','213141','jjjj',{'Matangi'});
Insert into driver(driver_name,current_position,mobile,password,skills) values('Milan','Upper Hutt','211111','jjjj1',{'Matangi'});
Insert into driver(driver_name,current_position,mobile,password,skills) values('Pavle','Upper Hutt','2133344','jjjj2',{'Ganz Mavag,Guliver'});

---Create new data for vehicle table
consistency;
Insert into vehicle(vehicle_id,status,type) values('FA1122','Upper Hutt','Matangi');
Insert into vehicle(vehicle_id,status,type) values('FP8899','Wellington','Ganz Mavag');
Insert into vehicle(vehicle_id,status,type) values('FA4864','Wellington','Matangi');
Insert into vehicle(vehicle_id,status,type) values('KW3300','Wellington','Matangi');
Insert into vehicle(vehicle_id,status,type) values('FA8899','Lower Hutt','Gulliver');

---Create new data for vehicle distance table
consistency;
Insert into vehicle_distance(vehicle_id,date,daily_distance, total_distance) values('FA8899',20170328,200,2362.2);
Insert into vehicle_distance(vehicle_id,date,daily_distance, total_distance) values('KW3300',20170401,200,2362.2);
Insert into vehicle_distance(vehicle_id,date,daily_distance, total_distance) values('KW3300',20170402,200,2562.2);

---Create new data timetable
consistency;
insert into time_table(line_name,service_no,latitude,time,distance,station_name) values ('Hutt_Vale_Line',1,-41.2865,0605,0,'Wellington');
insert into time_table(line_name,service_no,latitude,time,distance,station_name) values ('Hutt_Vale_Line',1,-41.2227,0617,8.3,'Petone');
insert into time_table(line_name,service_no,latitude,time,distance,station_name) values ('Hutt_Vale_Line',1,-41.2092,0625,11,'Waterloo');
insert into time_table(line_name,service_no,latitude,time,distance,station_name) values ('Hutt_Vale_Line',1,-41.1798,0634,19,'Taita');
insert into time_table(line_name,service_no,latitude,time,distance,station_name) values ('Hutt_Vale_Line',1,-41.1539,0642,26.5,'Silverstream');
insert into time_table(line_name,service_no,latitude,time,distance,station_name) values ('Hutt_Vale_Line',1,-41.1244,0650,34.3,'UpperHutt');
insert into time_table(line_name,service_no,latitude,time,distance,station_name) values ('Hutt_Vale_Line',11,-41.2865,1935,0,'Wellington');
insert into time_table(line_name,service_no,latitude,time,distance,station_name) values ('Hutt_Vale_Line',11,-41.2227,1947,8.3,'Petone');
insert into time_table(line_name,service_no,latitude,time,distance,station_name) values ('Hutt_Vale_Line',11,-41.2092,1955,11,'Waterloo');
insert into time_table(line_name,service_no,latitude,time,distance,station_name) values ('Hutt_Vale_Line',11,-41.1798,2010,19,'Taita');
insert into time_table(line_name,service_no,latitude,time,distance,station_name) values ('Hutt_Vale_Line',11,-41.1539,2019,26.5,'Silverstream');
insert into time_table(line_name,service_no,latitude,time,distance,station_name) values ('Hutt_Vale_Line',11,-41.1244,2025,34.3,'UpperHutt');

---Create new data departure stations timetable
consistency;
insert into dept_stat_time_table(line_name,service_no,time,dept_stat_name) values ('Hutt_Vale_Line',1,0605,'Wellington');
insert into dept_stat_time_table(line_name,service_no,time,dept_stat_name) values ('Hutt_Vale_Line',5,1025,'Wellington');
insert into dept_stat_time_table(line_name,service_no,time,dept_stat_name) values ('Hutt_Vale_Line',11,1935,'Wellington');

---Create new data for data point table
consistency;
insert into data_point(line_name,service_no,day,sequence,latitude,longitude,speed) values ('Hutt_Vale_Line',1,20170403,'2017-04-03 10:47:50',-41.2202,174.98,29.8);
insert into data_point(line_name,service_no,day,sequence,latitude,longitude,speed) values ('Hutt_Vale_Line',1,20170403,'2017-04-03 10:37:50',-41.2262,174.77,29.1);
insert into data_point(line_name,service_no,day,sequence,latitude,longitude,speed) values ('Hutt_Vale_Line',5,20170403,'2017-04-03 10:49:50',-41.1251,174.99,32.1);
insert into data_point(line_name,service_no,day,sequence,latitude,longitude,speed) values ('Hutt_Vale_Line',5,20170403,'2017-04-03 10:39:50',-41.1213,175,31);

consistency;
----1) Change driver's password by driver_name.
update driver set password = 'Jane new password' where driver_name='Jane';
update driver set password = 'Milan new password' where driver_name='Milan';
update driver set password = 'Pavle new password' where driver_name='Pavle';

consistency;
----2) Update driver's current_position by station's name or vehicle's ID or 'not_available'.
update driver set current_position = 'Wellington' where driver_name='Jane';
update driver set current_position = 'FP8899' where driver_name='Milan';
update driver set current_position = 'not_available' where driver_name='Pavle';

consistency;
----3) Update driver's skills.
update driver set skills = skills + {'Kiwi Rail'} where driver_name='Jane';
update driver set skills = skills + {'Ganz Mavag'} where driver_name='Milan';
update driver set skills = skills + {'Matangi'} where driver_name='Pavle';

consistency;
----4) pdate vehicle 's status by the station name or 'in use' or 'maintenance' or 'out_of_order'.
update vehicle set status = 'Wellington' where vehicle_id='KW3300';
update vehicle set status = 'in_use' where vehicle_id='FA8899';
update vehicle set status = 'out_of_order' where vehicle_id='FA4864';
update vehicle set status = 'maintenance' where vehicle_id='FP8899';

consistency;
----5) Update vehicle 's daily distance and total distance.
update vehicle_distance set daily_distance = 34.5, total_distance = 2396.7 where vehicle_id='FA8899' and date=20170403;
update vehicle_distance set daily_distance = 100, total_distance = 2496.7 where vehicle_id='FA8899' and date=20170402;
update vehicle_distance set daily_distance = 200, total_distance = 2696.7 where vehicle_id='FA8899' and date=20170401;

consistency;
----6) Record data point information.
insert into data_point(line_name,service_no,day,sequence,latitude,longitude,speed) values ('Hutt_Vale_Line',1,20170403,'2017-04-03 10:57:50',-41.2154,174.98,29.8);
insert into data_point(line_name,service_no,day,sequence,latitude,longitude,speed) values ('Hutt_Vale_Line',1,20170403,'2017-04-03 11:07:50',-41.2103,174.77,29.1);
insert into data_point(line_name,service_no,day,sequence,latitude,longitude,speed) values ('Hutt_Vale_Line',5,20170403,'2017-04-03 10:59:50',-41.1293,174.99,32.1);
insert into data_point(line_name,service_no,day,sequence,latitude,longitude,speed) values ('Hutt_Vale_Line',5,20170403,'2017-04-03 11:09:50',-41.1342,175,31.8);

consistency;
----7) Record the number of days per month a driver registered.
update driver_attendance set monthly_days = monthly_days + 19 where driver_name='Jane' and month=201702;
update driver_attendance set monthly_days = monthly_days + 20 where driver_name='Jane' and month=201703;
update driver_attendance set monthly_days = monthly_days + 1 where driver_name='Jane' and month=201704;
