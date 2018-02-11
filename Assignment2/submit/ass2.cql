ccm create -n 5 single_dc
CREATE KEYSPACE IF NOT EXISTS ass2 WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':3};

SOURCE '~/private/a2/table_declarations.cql'
COPY driver(driver_name,current_position,email,mobile,password,skill) FROM  '~/private/a2/driver_data.txt' ;
COPY vehicle(vehicle_id,status,type) FROM  '~/private/a2/vehicle_data.txt' ;
COPY time_table(line_name,service_no,time,distance,latitude,longitude,stop) FROM  '~/private/a2/time_table_data.txt' ;
COPY data_point(line_name,service_no,date,sequence,latitude,longitude,speed) FROM  '~/private/a2/data_point_data.txt' ;

select * from driver where driver_name='pavle';
select * from vehicle where vehicle_id='FA1122';
select * from data_point where line_name='Hutt Valey Line' and service_no=2 and date=20160322;
select * from time_table where line_name='Hutt Valley Line' and service_no=2 and time=1045;

ccm node1 nodetool getendpoints ass2 driver pavle

ccm create -n 5:4 -s multi_dc
CREATE KEYSPACE IF NOT EXISTS ass2 WITH REPLICATION={'class':'NetworkTopologyStrategy', 'dc1':3, 'dc2':3};

SOURCE '~/private/a2/table_declarations.cql'
COPY driver(driver_name,current_position,email,mobile,password,skill) FROM  '~/private/a2/driver_data.txt' ;
COPY time_table(line_name,service_no,time,distance,latitude,longitude,stop) FROM  '~/private/a2/time_table_data.txt' ;
