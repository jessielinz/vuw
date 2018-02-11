ssh -X lijins@regent.ecs.vuw.ac.nz
ssh -X lijins@greta-pt.ecs.vuw.ac.nz
single-mongo start
rep-mongo
sha-mongo
sharep-mongo

single-mongo stop;single-mongo cleanall;
rep-mongo stop;rep-mongo cleanall;
sha-mongo stop;sha-mongo cleanall;
sharep-mongo stop;sharep-mongo cleanall;

sha-mongo init 5;


mongoimport --db test --collection reserves --file ~/private/a4/ass4_reserves_17.txt
db.reserves.find({"marina.name":{$regex: /^Port/}},{ "marina.name": 1, "reserves.boat.number": 1,"reserves.sailor.sailorId":1 })

1)
db.reserves.aggregate([{$match:{"reserves.sailor.sailorId": {$ne:null}}},{$group: {_id: "$reserves.sailor.sailorId", name: {$first:"$reserves.sailor.name"}, skills: {$first:"$reserves.sailor.skills"}, address: {$first:"$reserves.sailor.address"}}}])

db.reserves.aggregate([{$match:{"reserves.sailor.sailorId": {$exists: true}}},{$group: {_id: "$reserves.sailor.sailorId", name: {$first:"$reserves.sailor.name"}, skills: {$first:"$reserves.sailor.skills"}, address: {$first:"$reserves.sailor.address"}}}])

db.reserves.aggregate([{$match:{_id: {$exists: true}}},{$group: {_id: "$reserves.sailor.sailorId", name: {$first:"$reserves.sailor.name"}, skills: {$first:"$reserves.sailor.skills"}, address: {$first:"$reserves.sailor.address"}}}])
db.reserves.aggregate([{$project: {_id: "$reserves.sailor.sailorId", name: "$reserves.sailor.name", skills: "$reserves.sailor.skills", address: "$reserves.sailor.address"}}])
db.reserves.aggregate([{$project: {_id: "$reserves.sailor.sailorId", name: "$reserves.sailor.name", skills: "$reserves.sailor.skills", address: "$reserves.sailor.address"}},])
db.reserves.aggregate([{$match:{$not: _id}},{$group: {_id: "$reserves.sailor.sailorId", name: {$first:"$reserves.sailor.name"}, skills: {$first:"$reserves.sailor.skills"}, address: {$first:"$reserves.sailor.address"}}}])

db.reserves.aggregate([{$match:{"reserves.sailor.sailorId": {$ne:null}}}])

2)
db.reserves.aggregate([{$group: {_id: "$reserves.sailor.sailorId", name: {$first:"$reserves.sailor.name"}, address: {$first:"$reserves.sailor.address"}, no_of_reserves: {$sum: 1}}},{$match:{_id: {$ne:null}}},{$sort: {no_of_reserves: -1}},{$limit: 1},{$project: {sailorId: "$_id",name: "$name", address:"$address", no_of_reserves: "$no_of_reserves", _id: 0}}])

db.reserves.aggregate([{$group: {_id: "$reserves.sailor.sailorId", sailorId: {$first:"$reserves.sailor.sailorId"}, name: {$first:"$reserves.sailor.name"}, address: {$first:"$reserves.sailor.address"}, no_of_reserves: {$sum: 1}}},{$match:{_id: {$ne:null}}},{$sort: {no_of_reserves: -1}},{$limit: 1},{$project: {_id: 0, sailorId: 1, name: 1, address: 1, no_of_reserves:1}}])

3)
db.reserves.aggregate([{$group: {_id: 1, total_reserves: {$sum: 1}}}])
db.reserves.aggregate([{$group: {_id: null, total_reserves: {$sum: 1}}}, {$project: {_id: 0, total_reserves: 1}}])
db.reserves.aggregate([{$match:{"reserves.sailor.sailorId": {$ne:null}}},{$group: {_id: null, total_reserves: {$sum: 1}}}, {$project: {_id: 0, total_reserves: 1}}])

4)
db.reserves.aggregate(
   [
     {
       $match:
         {
           "reserves.sailor.sailorId": {$ne:null}
         }
     },
     {
       $group:
         {
           _id: "$reserves.sailor.sailorId",
           no_of_reserves: {$sum: 1}
         }
     },
     {
       $group:
         {
           _id: 1,
           no_of_sailors: {$sum: 1},
           total_reserves: {$sum: "$no_of_reserves"}
         }
     },
     {
       $project:
         {
           _id: 0,
           avg_reserves_by_sailors: {$divide: ["$total_reserves", "$no_of_sailors"]}
         }
     }
   ]
)

db.reserves.aggregate([
     {$match:{"reserves.sailor.sailorId": {$ne:null}}},
     {$group:{_id: "$reserves.sailor.sailorId", no_of_reserves: {$sum: 1}}},
     {$group:{_id: null, no_of_sailors: {$sum: 1},total_reserves: {$sum: "$no_of_reserves"}}},
     {$project:{_id: 0, avg_reserves_by_sailors: {$divide: ["$total_reserves", "$no_of_sailors"]}}}
   ])

db.reserves.aggregate([
   {$match:{"reserves.sailor.sailorId": {$ne:null}}},
   {$group:{_id: "$reserves.sailor.sailorId", no_of_reserves: {$sum: { $cond: [ {$not: ["$reserves.date"]}, 0, 1 ] }}}},
   {$group:{_id: null, no_of_sailors: {$sum: 1},total_reserves: {$sum: "$no_of_reserves"}}},
   {$project:{_id: 0, avg_reserves_by_sailors: {$divide: ["$total_reserves", "$no_of_sailors"]}}}
 ])

Q5)
[ "Killer Whale", "Penguin", "Night Breeze", "Sea Gull" ]

db.reserves.aggregate([
     {$match:{"reserves.boat.driven_by": {$ne:null}}},
     {$group:{_id: "$reserves.boat.number", name: {$first:"$reserves.boat.name"}, driven_by: {$first:"$reserves.boat.driven_by"}}}
   ])

db.reserves.aggregate([
    {$match:{"reserves.sailor.sailorId": {$ne:null}}},
    {$group: {_id: "$reserves.sailor.sailorId", name: {$first:"$reserves.sailor.name"}, skills: {$first:"$reserves.sailor.skills"}}}
    ])

db.reserves.distinct("reserves.boat.name", {"reserves.boat.driven_by": {$in: ["sail", "motor"], $exists: true}})

var curs = db.reserves.aggregate([
    {$match:{"reserves.sailor.name": "Paul"}},
    {$group: {_id: "$reserves.sailor.sailorId", name: {$first:"$reserves.sailor.name"}, skills: {$first:"$reserves.sailor.skills"}}}
    ])
while (curs.hasNext()) {
... tmp_sailor=curs.next();
... tmp_boat=db.reserves.distinct("reserves.boat.name", {"reserves.boat.driven_by": {$in: tmp_sailor.skills}});
... ret={sailor_name:tmp_sailor.name, boats_name:tmp_boat};
... print(tojson(ret));
... }

var curs= db.reserves.aggregate([
    {$match:{"reserves.sailor.name": "Paul"}},
    {$group: {_id: "$reserves.sailor.name", skills: {$first:"$reserves.sailor.skills"}}}
    ])

var curs1 = db.reserves.aggregate({$match:{"reserves.boat.name": {$exists: true}}},{$group: {_id: "$reserves.boat.name", driven_by: {$first:"$reserves.boat.driven_by"}}});

if (curs.hasNext()) {
    tmp_sailor=curs.next();
    a2 = tmp_sailor.skills;
    var boat_array = [];
    while (curs1.hasNext()) {
        tmp_boat=curs1.next();
        a1 = tmp_boat.driven_by;
        if (a1.length<=a2.length && a1.every(function(v,i) { return v === a2[i]})) {
            boat_array.push(tmp_boat._id);
        }
    }
    print(tojson(boat_array));
}


Bonus)
db.reserves.aggregate([
{$group: {_id: "$reserves.sailor.sailorId", name: {$first:"$reserves.sailor.name"}, no_of_reserves: {$sum: 1}}},
{$project: {sailorId: "$_id",name: "$name", address:"$address", no_of_reserves: "$no_of_reserves", _id: 0}},
{$match:{$and :[{_id: {$ne:null}},{"$no_of_reserves": {$gt: 2.7}}]}}]);

db.reserves.aggregate([
{$group: {_id: "$reserves.sailor.sailorId", name: {$first:"$reserves.sailor.name"}, no_of_reserves: {$sum: 1}}},
{$match:{"$no_of_reserves": 3}},
{$project: {sailorId: "$_id",name: "$name", address:"$address", no_of_reserves: "$no_of_reserves", _id: 0}}]);

var curs1 = db.reserves.aggregate([
     {$match:{"reserves.sailor.sailorId": {$ne:null}}},
     {$group:{_id: "$reserves.sailor.sailorId", no_of_reserves: {$sum: 1}}},
     {$group:{_id: 1, no_of_sailors: {$sum: 1},total_reserves: {$sum: "$no_of_reserves"}}},
     {$project:{_id: 0, avg_reserves_by_sailors: {$divide: ["$total_reserves", "$no_of_sailors"]}}}
   ]);
curs1.hasNext();
tmp_avg=curs1.next();
var avg_no_res = tmp_avg.avg_reserves_by_sailors;
//print(tojson(avg_no_res));

var curs = db.reserves.aggregate([
     {$group: {_id: "$reserves.sailor.sailorId", name: {$first:"$reserves.sailor.name"}, no_of_reserves: {$sum: 1}}},
     {$match:{_id: {$ne:null}}},
     {$project: {name: "$name", no_of_reserves: "$no_of_reserves", _id: 0}}]);

 while (curs.hasNext()) {
    tmp_sailor=curs.next();
    //print(tojson(tmp_sailor.name));
    var ret;
    if (tmp_sailor.no_of_reserves > avg_no_res) {
      ret={sailor_name:tmp_sailor.name};
      print(tojson(ret));
    }
 }
Q6)
sha-mongo init 2
sha-mongo test

mongo
use mydb
db.user.getShardDistribution()

Shard shard0000 at 127.0.0.1:27020
 data : 5.33MiB docs : 50000 chunks : 6
 estimated data per chunk : 911KiB
 estimated docs per chunk : 8333

Shard shard0001 at 127.0.0.1:27021
 data : 5.33MiB docs : 50000 chunks : 6
 estimated data per chunk : 911KiB
 estimated docs per chunk : 8333

Totals
 data : 10.68MiB docs : 100000 chunks : 12
 Shard shard0000 contains 50% data, 50% docs in cluster, avg obj size on shard : 112B
 Shard shard0001 contains 50% data, 50% docs in cluster, avg obj size on shard : 112B

 mongos> db.printShardingStatus(true)
 --- Sharding Status ---
   sharding version: {
         "_id" : 1,
         "version" : 4,
         "minCompatibleVersion" : 4,
         "currentVersion" : 5,
         "clusterId" : ObjectId("59238ab5546fb27fc5964e19")
 }
   shards:
         {  "_id" : "shard0000",  "host" : "127.0.0.1:27020" }
         {  "_id" : "shard0001",  "host" : "127.0.0.1:27021" }
   databases:
         {  "_id" : "admin",  "partitioned" : false,  "primary" : "config" }
         {  "_id" : "mydb",  "partitioned" : true,  "primary" : "shard0000" }
                 mydb.user
                         shard key: { "user_id" : 1 }
                         chunks:
                                 shard0001       6
                                 shard0000       6
                         { "user_id" : { "$minKey" : 1 } } -->> { "user_id" : 0 } on : shard0001 Timestamp(12, 0)
                         { "user_id" : 0 } -->> { "user_id" : 5999 } on : shard0000 Timestamp(12, 1)
                         { "user_id" : 5999 } -->> { "user_id" : 15999 } on : shard0001 Timestamp(11, 1)
                         { "user_id" : 15999 } -->> { "user_id" : 25999 } on : shard0000 Timestamp(3, 2)
                         { "user_id" : 25999 } -->> { "user_id" : 35999 } on : shard0001 Timestamp(4, 2)
                         { "user_id" : 35999 } -->> { "user_id" : 45999 } on : shard0000 Timestamp(5, 2)
                         { "user_id" : 45999 } -->> { "user_id" : 55999 } on : shard0001 Timestamp(6, 2)
                         { "user_id" : 55999 } -->> { "user_id" : 65999 } on : shard0000 Timestamp(7, 2)
                         { "user_id" : 65999 } -->> { "user_id" : 75999 } on : shard0001 Timestamp(8, 2)
                         { "user_id" : 75999 } -->> { "user_id" : 85999 } on : shard0000 Timestamp(9, 2)
                         { "user_id" : 85999 } -->> { "user_id" : 95999 } on : shard0001 Timestamp(10, 2)
                         { "user_id" : 95999 } -->> { "user_id" : { "$maxKey" : 1 } } on : shard0000 Timestamp(11, 0)
 =======================================================================================
 sha-mongo init 5
 sha-mongo status
 regent: [~] % sha-mongo status;
MongoDB shell version: 2.6.7
connecting to: 127.0.0.1:27017/test
--- Sharding Status ---
  sharding version: {
        "_id" : 1,
        "version" : 4,
        "minCompatibleVersion" : 4,
        "currentVersion" : 5,
        "clusterId" : ObjectId("5926151d03984f6247a22c7e")
}
  shards:
        {  "_id" : "shard0000",  "host" : "127.0.0.1:27020" }
        {  "_id" : "shard0001",  "host" : "127.0.0.1:27021" }
        {  "_id" : "shard0002",  "host" : "127.0.0.1:27022" }
        {  "_id" : "shard0003",  "host" : "127.0.0.1:27023" }
        {  "_id" : "shard0004",  "host" : "127.0.0.1:27024" }
  databases:
        {  "_id" : "admin",  "partitioned" : false,  "primary" : "config" }
        {  "_id" : "mydb",  "partitioned" : true,  "primary" : "shard0000" }
                mydb.user
                        shard key: { "user_id" : 1 }
                        chunks:
                                shard0000       3
                                shard0001       3
                                shard0002       2
                                shard0003       2
                                shard0004       2
                        { "user_id" : { "$minKey" : 1 } } -->> { "user_id" : 0 } on : shard0000 Timestamp(11, 1)
                        { "user_id" : 0 } -->> { "user_id" : 5999 } on : shard0000 Timestamp(1, 3)
                        { "user_id" : 5999 } -->> { "user_id" : 15999 } on : shard0001 Timestamp(7, 1)
                        { "user_id" : 15999 } -->> { "user_id" : 25999 } on : shard0002 Timestamp(8, 1)
                        { "user_id" : 25999 } -->> { "user_id" : 35999 } on : shard0003 Timestamp(9, 1)
                        { "user_id" : 35999 } -->> { "user_id" : 45999 } on : shard0004 Timestamp(10, 1)
                        { "user_id" : 45999 } -->> { "user_id" : 55999 } on : shard0001 Timestamp(6, 2)
                        { "user_id" : 55999 } -->> { "user_id" : 65999 } on : shard0002 Timestamp(7, 2)
                        { "user_id" : 65999 } -->> { "user_id" : 75999 } on : shard0003 Timestamp(8, 2)
                        { "user_id" : 75999 } -->> { "user_id" : 85999 } on : shard0004 Timestamp(9, 2)
                        { "user_id" : 85999 } -->> { "user_id" : 95999 } on : shard0000 Timestamp(10, 2)
                        { "user_id" : 95999 } -->> { "user_id" : { "$maxKey" : 1 } } on : shard0001 Timestamp(11, 0)

 db.user.getShardDistribution()

 Shard shard0000 at 127.0.0.1:27020
 data : 1.7MiB docs : 15999 chunks : 3
 estimated data per chunk : 583KiB
 estimated docs per chunk : 5333

Shard shard0001 at 127.0.0.1:27021
 data : 2.56MiB docs : 24001 chunks : 3
 estimated data per chunk : 875KiB
 estimated docs per chunk : 8000

Shard shard0002 at 127.0.0.1:27022
 data : 2.13MiB docs : 20000 chunks : 2
 estimated data per chunk : 1.06MiB
 estimated docs per chunk : 10000

Shard shard0003 at 127.0.0.1:27023
 data : 2.13MiB docs : 20000 chunks : 2
 estimated data per chunk : 1.06MiB
 estimated docs per chunk : 10000

Shard shard0004 at 127.0.0.1:27024
 data : 2.13MiB docs : 20000 chunks : 2
 estimated data per chunk : 1.06MiB
 estimated docs per chunk : 10000

Totals
 data : 10.68MiB docs : 100000 chunks : 12
 Shard shard0000 contains 15.99% data, 15.99% docs in cluster, avg obj size on shard : 112B
 Shard shard0001 contains 24% data, 24% docs in cluster, avg obj size on shard : 112B
 Shard shard0002 contains 20% data, 20% docs in cluster, avg obj size on shard : 112B
 Shard shard0003 contains 20% data, 20% docs in cluster, avg obj size on shard : 112B
 Shard shard0004 contains 20% data, 20% docs in cluster, avg obj size on shard : 112B

mongos> db.printShardingStatus(true)
--- Sharding Status ---
  sharding version: {
        "_id" : 1,
        "version" : 4,
        "minCompatibleVersion" : 4,
        "currentVersion" : 5,
        "clusterId" : ObjectId("59238ab5546fb27fc5964e19")
}
  shards:
        {  "_id" : "shard0000",  "host" : "127.0.0.1:27020" }
        {  "_id" : "shard0001",  "host" : "127.0.0.1:27021" }
        {  "_id" : "shard0002",  "host" : "127.0.0.1:27022" }
        {  "_id" : "shard0003",  "host" : "127.0.0.1:27023" }
        {  "_id" : "shard0004",  "host" : "127.0.0.1:27024" }
  databases:
        {  "_id" : "admin",  "partitioned" : false,  "primary" : "config" }
        {  "_id" : "mydb",  "partitioned" : true,  "primary" : "shard0000" }
                mydb.user
                        shard key: { "user_id" : 1 }
                        chunks:
                                shard0003       6
                                shard0002       6
                                shard0004       6
                                shard0001       7
                                shard0000       6
                        { "user_id" : { "$minKey" : 1 } } -->> { "user_id" : 0 } on : shard0003 Timestamp(14, 0)
                        { "user_id" : 0 } -->> { "user_id" : 2340 } on : shard0002 Timestamp(18, 2)
                        { "user_id" : 2340 } -->> { "user_id" : 5999 } on : shard0002 Timestamp(18, 3)
                        { "user_id" : 5999 } -->> { "user_id" : 8679 } on : shard0002 Timestamp(18, 4)
                        { "user_id" : 8679 } -->> { "user_id" : 11019 } on : shard0002 Timestamp(18, 6)
                        { "user_id" : 11019 } -->> { "user_id" : 15999 } on : shard0002 Timestamp(18, 7)
                        { "user_id" : 15999 } -->> { "user_id" : 18679 } on : shard0004 Timestamp(18, 8)
                        { "user_id" : 18679 } -->> { "user_id" : 21019 } on : shard0004 Timestamp(18, 10)
                        { "user_id" : 21019 } -->> { "user_id" : 25999 } on : shard0004 Timestamp(18, 11)
                        { "user_id" : 25999 } -->> { "user_id" : 28339 } on : shard0004 Timestamp(18, 12)
                        { "user_id" : 28339 } -->> { "user_id" : 30679 } on : shard0004 Timestamp(18, 14)
                        { "user_id" : 30679 } -->> { "user_id" : 35999 } on : shard0004 Timestamp(18, 15)
                        { "user_id" : 35999 } -->> { "user_id" : 38339 } on : shard0003 Timestamp(18, 16)
                        { "user_id" : 38339 } -->> { "user_id" : 40679 } on : shard0003 Timestamp(18, 18)
                        { "user_id" : 40679 } -->> { "user_id" : 45999 } on : shard0003 Timestamp(18, 19)
                        { "user_id" : 45999 } -->> { "user_id" : 49679 } on : shard0003 Timestamp(19, 0)
                        { "user_id" : 49679 } -->> { "user_id" : 52019 } on : shard0002 Timestamp(20, 0)
                        { "user_id" : 52019 } -->> { "user_id" : 55999 } on : shard0001 Timestamp(20, 1)
                        { "user_id" : 55999 } -->> { "user_id" : 58339 } on : shard0003 Timestamp(21, 0)
                        { "user_id" : 58339 } -->> { "user_id" : 60679 } on : shard0000 Timestamp(21, 1)
                        { "user_id" : 60679 } -->> { "user_id" : 65999 } on : shard0000 Timestamp(18, 27)
                        { "user_id" : 65999 } -->> { "user_id" : 69679 } on : shard0001 Timestamp(18, 28)
                        { "user_id" : 69679 } -->> { "user_id" : 72019 } on : shard0001 Timestamp(18, 30)
                        { "user_id" : 72019 } -->> { "user_id" : 75999 } on : shard0001 Timestamp(18, 31)
                        { "user_id" : 75999 } -->> { "user_id" : 79679 } on : shard0000 Timestamp(18, 32)
                        { "user_id" : 79679 } -->> { "user_id" : 82019 } on : shard0000 Timestamp(18, 34)
                        { "user_id" : 82019 } -->> { "user_id" : 85999 } on : shard0000 Timestamp(18, 35)
                        { "user_id" : 85999 } -->> { "user_id" : 89679 } on : shard0001 Timestamp(18, 36)
                        { "user_id" : 89679 } -->> { "user_id" : 92019 } on : shard0001 Timestamp(18, 38)
                        { "user_id" : 92019 } -->> { "user_id" : 95999 } on : shard0001 Timestamp(18, 39)
                        { "user_id" : 95999 } -->> { "user_id" : { "$maxKey" : 1 } } on : shard0000 Timestamp(11, 0)

Q6)-c)
mongo --host  127.0.0.1:27025


Q6)-d)
db.user.find({"user_id":55555});

 Q7)
sharep-mongo connect 0 0

use admin
rs.initiate()
rs.status()
