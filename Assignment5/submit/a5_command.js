ssh -X lijins@regent.ecs.vuw.ac.nz
ssh -X lijins@greta-pt.ecs.vuw.ac.nz

--dropdb
createdb book_orders_db_lijins
psql -d book_orders_db_lijins -f ~/private/a5/BookOrdersDatabaseDump_17.sql

need postgresql
psql -d book_orders_db_lijins

CREATE TABLE customer_dim (
    customerid integer primary key,
    l_name character(20) NOT NULL,
    f_name character(20),
    city character(15) NOT NULL,
    district character(15) NOT NULL,
    country character(15) NOT NULL,
    CONSTRAINT customer_customerid CHECK ((customerid > 0))
);

CREATE Table time_dim(
	timeid SERIAL primary key,
	orderdate date not null,
	dayofweek char(10) not null,
	month char(10) not null,
	year int not null
);

CREATE TABLE book_dim (
    isbn integer primary key,
    title character(60) NOT NULL,
    edition_no smallint DEFAULT 1,
    price numeric(6,2) NOT NULL,
    CONSTRAINT book_edition_no CHECK ((edition_no > 0)),
    CONSTRAINT book_price CHECK ((price > (0)::numeric))
);

insert into book_dim (isbn, title, edition_no, price) (
  select isbn, title, edition_no, price
  from book
);

CREATE table sales_fact(
	customerid int NOT NULL,
	timeid int NOT NULL,
    isbn int NOT NULL,
    amnt numeric(6,2) NOT NULL,
	FOREIGN KEY (customerid) REFERENCES customer_dim(customerid),
	FOREIGN KEY (timeid) REFERENCES time_dim(timeid),
  FOREIGN KEY (isbn) REFERENCES book_dim(isbn),
  CONSTRAINT amnt CHECK ((customerid > (0)::int)),
  CONSTRAINT amnt CHECK ((timeid > (0)::int)),
  CONSTRAINT amnt CHECK ((isbn > (0)::int)),
  CONSTRAINT amnt CHECK ((amnt > (0)::numeric))
);

ALTER TABLE ONLY sales_fact
    ADD CONSTRAINT sales_fact_pkey PRIMARY KEY (customerid, timeid, isbn);

select to_char(orderdate,'Day'),to_char(orderdate,'Month'), to_char(orderdate,'YYYY'), orderdate from cust_order order by orderdate limit 5;
select * from time_dim;

insert into customer_dim (customerid, l_name, f_name, city, district, country) (
  select customerid, l_name, f_name, city, district, country
  from customer
);

insert into time_dim (orderdate, dayofweek, month, year) (
  select orderdate, to_char(orderdate,'Day'),to_char(orderdate,'Month'), to_number(to_char(orderdate,'YYYY'),'9999')
  from cust_order
  order by orderdate
);

insert into book_dim (isbn, title, edition_no, price) (
  select isbn, title, edition_no, price
  from book
);

insert into sales_fact(customerid, timeid, isbn, amnt) (
  select customer_dim.customerid, time_dim.timeid, book_dim.isbn, order_detail.quantity * book_dim.price
  from order_detail,customer_dim,time_dim,book_dim
  where order_detail.isbn = book_dim.isbn
    and order_detail.orderid = cust_order.orderid
    and cust_order.orderdate = time_dim.orderdate
);

select customer_dim.customerid, time_dim.timeid, book_dim.isbn, order_detail.quantity * book_dim.price
from cust_order, order_detail,customer_dim,time_dim,book_dim
where order_detail.isbn = book_dim.isbn
  and order_detail.orderid = cust_order.orderid
  and cust_order.orderdate = time_dim.orderdate


select count(1) from (
  select cust_order_book_detail.customerid, time_dim.timeid, cust_order_book_detail.isbn, cust_order_book_detail.price as price, cust_order_book_detail.quantity * cust_order_book_detail.price as amnt from
  (select cust_order_detail.customerid, cust_order_detail.orderdate, cust_order_detail.isbn, cust_order_detail.quantity, book_dim.price from 
  	(select cust_order.customerid, cust_order.orderdate, order_detail.isbn, sum(order_detail.quantity) quantity  from cust_order 
  		natural join order_detail 
  		group by cust_order.customerid, cust_order.orderdate, order_detail.isbn) as cust_order_detail 
  natural join book_dim) as cust_order_book_detail 
  inner join time_dim on cust_order_book_detail.orderdate = time_dim.orderdate
) as tmp 
order by customerid, timeid, isbn, price;

select distinct cust_order.customerid, cust_order.orderdate, order_detail.isbn, order_detail.quantity from cust_order 
  		inner join order_detail on order_detail.orderid = cust_order.orderid
select count(1) from (
select cust_order.customerid, cust_order.orderdate, order_detail.isbn, sum(order_detail.quantity) quantity  from cust_order 
  		inner join order_detail on order_detail.orderid = cust_order.orderid
group by cust_order.customerid, cust_order.orderdate, order_detail.isbn
) as tmp;

select count(1) from cust_order 
  		inner join order_detail on order_detail.orderid = cust_order.orderid
select count(1) from 
(select cust_order_detail.customerid, cust_order_detail.orderdate, cust_order_detail.isbn, cust_order_detail.quantity, book_dim.price from 
  	(select cust_order.customerid, cust_order.orderdate, order_detail.isbn, sum(order_detail.quantity) quantity  from cust_order 
  		inner join order_detail on order_detail.orderid = cust_order.orderid 
  		group by cust_order.customerid, cust_order.orderdate, order_detail.isbn) as cust_order_detail 
  inner join book_dim on cust_order_detail.isbn = book_dim.isbn 
  order by cust_order_detail.customerid, cust_order_detail.orderdate, cust_order_detail.isbn) as cust_order_book_detail


select customerid, sum(amnt) from sales_fact
group by customerid
order by sum(amnt) desc
Limit 5;

Q3.
a)
create materialized view cust_sum_amnt_count_view as 
	select customerid, sum(amnt) as sum_amnt, rank() over (order by sum(amnt) desc) rank, count(1) from sales_fact 
	group by customerid;

select customer_dim.customerid,customer_dim.l_name,customer_dim.f_name from customer_dim 
natural join cust_sum_amnt_count_view 
where cust_sum_amnt_count_view.rank < 6;

--ord_avg_amnt
SELECT sum_amnt/no_order AS avg_order_amnt
FROM
  (SELECT 1 AS NO,
          sum(amnt) AS sum_amnt
   FROM sales_fact) AS tmp_a,

  (SELECT 1 AS NO,
          count(1) AS no_order
   FROM cust_order) AS tmp_b
WHERE tmp_a.no=tmp_b.no;

    avg_order_amnt
----------------------
 777.7702702702702703
(1 row)



--no_of_ord
select count(1) from cust_order natural join cust_sum_amnt_count_view 
where cust_sum_amnt_count_view.rank = 1;
count
-------
   14
(1 row)

--perc_of_ord
--caculate amount of per order, per customer
CREATE materialized VIEW cust_ord_amnt_view as
   SELECT cust_order.customerid, 
          ord_book_amnt.orderid, 
          SUM(ord_book_amnt.amnt) sum_amnt
   FROM   cust_order 
          NATURAL join (SELECT order_detail.orderid, 
                               order_detail.isbn, 
                               SUM(order_detail.quantity) * book_dim.price AS 
                               amnt 
                        FROM   order_detail 
                               NATURAL join book_dim 
                        GROUP  BY order_detail.orderid, 
                                  order_detail.isbn, 
                                  book_dim.price) AS ord_book_amnt 
   GROUP  BY cust_order.customerid, 
             ord_book_amnt.orderid;

select *
from cust_ord_amnt_view inner join cust_sum_amnt_count_view 
on cust_ord_amnt_view.customerid = cust_sum_amnt_count_view.customerid
where cust_sum_amnt_count_view.rank = 1
and cust_ord_amnt_view.sum_amnt > 777;

select count(1) from cust_ord_amnt_view where

CREATE OR REPLACE FUNCTION best_perc_of_ord() RETURNS numeric AS $$
DECLARE
sum_amnt numeric;
ord_count int;
avg_ord_amnt numeric;
per_of_ord numeric;
greater_count int;
no_of_ord int;
BEGIN
	--get customer rank by amnt
	drop materialized view if exists cust_sum_amnt_count_view;
	create materialized view cust_sum_amnt_count_view as 
	select customerid, sum(amnt) as sum_amnt, rank() over (order by sum(amnt) desc) rank, count(1) from sales_fact 
	group by customerid;
	--caculate amount of per order, per customer
	drop materialized view if exists cust_ord_amnt_view;
	CREATE materialized VIEW cust_ord_amnt_view as
   SELECT cust_order.customerid, 
          ord_book_amnt.orderid, 
          SUM(ord_book_amnt.amnt) sum_amnt
   FROM   cust_order 
          NATURAL join (SELECT order_detail.orderid, 
                               order_detail.isbn, 
                               SUM(order_detail.quantity) * book_dim.price AS 
                               amnt 
                        FROM   order_detail 
                               NATURAL join book_dim 
                        GROUP  BY order_detail.orderid, 
                                  order_detail.isbn, 
                                  book_dim.price) AS ord_book_amnt 
   GROUP  BY cust_order.customerid, 
             ord_book_amnt.orderid;
	sum_amnt = (SELECT sum(amnt) AS sum_amnt FROM sales_fact);
	ord_count = (SELECT count(1) AS ord_count FROM cust_order);
	avg_ord_amnt = sum_amnt/ord_count;
	--raise exception 'avg_ord_amnt: %', avg_ord_amnt;
	greater_count = (select count(1)
	from cust_ord_amnt_view inner join cust_sum_amnt_count_view 
	on cust_ord_amnt_view.customerid = cust_sum_amnt_count_view.customerid
	where cust_sum_amnt_count_view.rank = 1
	and cust_ord_amnt_view.sum_amnt > avg_ord_amnt);
	--raise exception 'greater_count: %', greater_count;
	no_of_ord = (select count(1) from cust_order natural join cust_sum_amnt_count_view 
	where cust_sum_amnt_count_view.rank = 1);
	--raise exception 'no_of_ord: %', no_of_ord;
	per_of_ord = greater_count/no_of_ord :: numeric;
	raise exception 'per_of_ord: %', per_of_ord;
	RETURN per_of_ord;
END;
$$ LANGUAGE plpgsql;

SELECT customerid,
          sum(amnt) AS sum_amnt,
          rank() over (
                       ORDER BY sum(amnt) DESC) rank,
                 count(1)
   FROM sales_fact
   GROUP BY customerid) AS tmp_view
WHERE tmp_view.rank < 6;

drop  materialized view if exists View1 CASCADE ;
CREATE MATERIALIZED VIEW View1 AS
SELECT c.CustomerId, F_Name, L_Name, District, TimeId,
DayOfWeek, ISBN, Amnt
FROM Sales_fact NATURAL JOIN Customer_dim c NATURAL JOIN Time_dim;

drop  materialized view if exists View2 CASCADE;
CREATE MATERIALIZED VIEW View2 AS
SELECT c.CustomerId, F_Name, L_Name, Year, SUM(Amnt)
FROM Sales_fact NATURAL JOIN Customer_dim c NATURAL JOIN Time_dim
GROUP BY c.CustomerId, F_Name, L_Name, Year;

drop  materialized view if exists View3 CASCADE;
CREATE MATERIALIZED VIEW View3 AS
SELECT District, TimeId, DayOfWeek, ISBN, SUM(Amnt)
FROM Sales_fact NATURAL JOIN Customer NATURAL JOIN Time_Dim
GROUP BY District, TimeId, DayOfWeek, ISBN;

SELECT View1.CustomerId,
       View1.F_Name,
       View1.L_Name,
       sum(amnt) AS sum_amnt,
       rank() over (
                    ORDER BY sum(amnt) DESC) rank
FROM View1
GROUP BY View1.CustomerId,
         View1.F_Name,
         View1.L_Name
LIMIT 5;

SELECT View2.CustomerId,
       View2.F_Name,
       View2.L_Name,
       sum(sum) AS sum_amnt,
       rank() over (
                    ORDER BY sum(sum) DESC) rank
FROM View2
GROUP BY View2.CustomerId,
         View2.F_Name,
         View2.L_Name
LIMIT 5;

SELECT sum(sum), country
FROM View3
NATURAL JOIN (select distinct district, country from customer_dim) as tmp_cust
GROUP BY country
ORDER BY sum(sum) DESC
LIMIT 1;

SELECT c.customerid,
       sum(b.price*od.quantity)
FROM customer c
NATURAL JOIN cust_order co
NATURAL JOIN order_detail od
NATURAL JOIN book b
GROUP BY c.customerid
ORDER BY sum(b.price*od.quantity) DESC
LIMIT 1;

SELECT c.country,
       sum(b.price*od.quantity)
FROM customer c
NATURAL JOIN cust_order co
NATURAL JOIN order_detail od
NATURAL JOIN book b
GROUP BY c.country
ORDER BY sum(b.price*od.quantity) DESC
LIMIT 1;

SELECT DISTINCT customerid, f_name, city, sum, avg
FROM
  ( SELECT customerid, f_name, city,
           sum(amnt) OVER (PARTITION BY customerid) as sum,
           avg(amnt) OVER (PARTITION BY city) as avg
   FROM sales_fact
   NATURAL JOIN time_dim
   NATURAL JOIN customer_dim
   WHERE (month= 'April'
          OR month= 'May')
     AND year=2017 ) AS tmp
ORDER BY city ;

SELECT DISTINCT customerid, f_name, city, sum, avg
FROM
  ( SELECT customerid, f_name, city,
           sum(amnt) OVER w1 as sum,
           avg(amnt) OVER w2 as avg
   FROM sales_fact
   NATURAL JOIN time_dim
   NATURAL JOIN customer_dim
   WHERE (month= 'April'
          OR month= 'May')
     AND year=2017 
   WINDOW w1 AS (PARTITION BY dayofweek),
          w2 AS (PARTITION BY city)
  ) AS tmp
ORDER BY city ;

SELECT DISTINCT city, timeid, orderdate as day, sum, cumulative_sum
FROM
  (SELECT city,
          timeid,
          orderdate,
          sum(amnt) OVER w1 AS sum,
          sum(amnt) OVER w2 AS cumulative_sum
   FROM sales_fact
   NATURAL JOIN time_dim
   NATURAL JOIN customer_dim
   NATURAL JOIN cust_order
   WHERE (month = 'April' OR month = 'May')
     AND year=2017 
   WINDOW w1 AS (PARTITION BY city,orderdate),
          w2 AS (PARTITION BY city ORDER BY orderdate) ) AS tmp
ORDER BY city, orderdate;

select customerid, f_name, city, amnt
   FROM sales_fact
   NATURAL JOIN customer_dim
   NATURAL JOIN time_dim
   where city='Auckland' and (month = 'April' OR month = 'May')
     AND year=2017 
order by customerid
limit 20;


SELECT DISTINCT customerid, f_name, city, sum, count, avg
FROM
  ( SELECT customerid, f_name, city,
           sum(amnt) OVER (PARTITION BY customerid) as sum,
           count(distinct(customerid)) ,
           avg(amnt) OVER (PARTITION BY city) as avg
   FROM sales_fact
   NATURAL JOIN customer_dim
   NATURAL JOIN time_dim
   WHERE (month= 'April' OR month= 'May')
     AND year=2017) AS tmp
ORDER BY city ;

select count(distinct(customerid)),city from sales_fact
   NATURAL JOIN customer
   NATURAL JOIN time_dim
   WHERE (month= 'April' OR month= 'May') AND year=2017
   group by city

select distinct(customerid),city from sales_fact
   NATURAL JOIN customer
   NATURAL JOIN time_dim
   WHERE (month= 'April' OR month= 'May') AND year=2017
   and city='Auckland'
SELECT first c.customerid,
       sum(b.price*od.quantity)
FROM customer c
NATURAL JOIN cust_order co
NATURAL JOIN order_detail od
NATURAL JOIN book b
GROUP BY c.customerid
ORDER BY sum(b.price*od.quantity) DESC

SELECT DISTINCT customerid, f_name, city, sum, avg
FROM
  ( SELECT customerid, f_name, city,
           sum(amnt) OVER (PARTITION BY customerid) as sum,
           avg(amnt) OVER (PARTITION BY city) as avg
   FROM sales_fact
   NATURAL JOIN customer_dim
   NATURAL JOIN time_dim
   WHERE (month= 'April' OR month= 'May')
     AND year=2017 ) AS tmp
ORDER BY city ;

select customerid, f_name, city, sum_amnt,avg(sum_amnt) OVER (PARTITION BY city) as avg from
(select customerid, f_name, city, sum(amnt) as sum_amnt
   FROM sales_fact
   NATURAL JOIN customer_dim
   NATURAL JOIN time_dim
   WHERE (month= 'April' OR month= 'May')
     AND year=2017 
group by customerid, f_name, city) as tmp
order by city


select customerid, f_name, city, sum(amnt)
   FROM sales_fact
   NATURAL JOIN customer_dim
   NATURAL JOIN time_dim
   where city='Auckland' and (orderdate = '2017-4-29')
     AND year=2017 
group by customerid, f_name, city
order by customerid

select customerid, f_name, city, orderdate, sum(amnt)
   FROM sales_fact
   NATURAL JOIN customer_dim
   NATURAL JOIN time_dim
   where city='Auckland' and (month = 'April' OR month = 'May')
     AND year=2017
group by customerid, f_name, city, orderdate
order by customerid
limit 20;

SELECT DISTINCT city, timeid, orderdate as day, daily_sum, 
                sum(daily_sum) OVER w2 AS cumulative_sum
FROM
  (SELECT city, timeid, orderdate,
          sum(amnt) OVER w1 AS daily_sum          
   FROM sales_fact
   NATURAL JOIN time_dim
   NATURAL JOIN customer_dim
   NATURAL JOIN cust_order
   WHERE (month = 'April' OR month = 'May')
     AND year=2017 
   WINDOW w1 AS (PARTITION BY city, orderdate) ) AS tmp
   WINDOW w2 AS (PARTITION BY city ORDER BY orderdate)
ORDER BY city, orderdate;

SELECT city, timeid, orderdate,
          sum(amnt) OVER w1 AS daily_sum          
   FROM sales_fact
   NATURAL JOIN time_dim
   NATURAL JOIN customer_dim
   NATURAL JOIN cust_order
   WHERE (month = 'April' OR month = 'May')
     AND year=2017 
   WINDOW w1 AS (PARTITION BY city, orderdate)

SELECT city, timeid, orderdate,
          sum(amnt) AS daily_sum          
   FROM sales_fact
   NATURAL JOIN time_dim
   NATURAL JOIN customer_dim
   WHERE (month = 'April' OR month = 'May')
     AND year=2017 
   group BY city, timeid, orderdate

select timeid, customerid, orderdate, sum(amnt) 
from sales_fact natural join time_dim natual natural join customer_dim
where customerid='94'
group by timeid, customerid, orderdate;

EXPLAIN ANALYZE
SELECT customer_dim.customerid,
       customer_dim.l_name,
       customer_dim.f_name
FROM customer_dim
NATURAL JOIN sales_fact
group by customer_dim.customerid,
       customer_dim.l_name,
       customer_dim.f_name
order by sum(amnt) desc
limit 5;

EXPLAIN ANALYZE
SELECT View1.CustomerId,
       View1.F_Name,
       View1.L_Name,
       sum(amnt) AS sum_amnt
FROM view1
GROUP BY View1.CustomerId,
         View1.F_Name,
         View1.L_Name
order by sum(amnt) desc
LIMIT 5;

EXPLAIN ANALYZE
SELECT View2.CustomerId,
       View2.F_Name,
       View2.L_Name,
       sum(SUM) AS sum_amnt
FROM View2
GROUP BY View2.CustomerId,
         View2.F_Name,
         View2.L_Name
order by sum(SUM) desc
LIMIT 5;

=============================================
EXPLAIN ANALYZE
SELECT country, sum(amnt)
FROM customer_dim
NATURAL JOIN sales_fact
GROUP BY country
ORDER BY sum(amnt) DESC
LIMIT 1;

EXPLAIN ANALYZE
SELECT country, sum(sum)
FROM View2
NATURAL JOIN customer_dim
GROUP BY country
ORDER BY sum(sum) DESC
LIMIT 1;

EXPLAIN ANALYZE
SELECT country, sum(sum)
FROM View3
NATURAL JOIN customer_dim
GROUP BY country
ORDER BY sum(sum) DESC
LIMIT 1;

VACCUM ANALYZE
SELECT country, sum(sum)
FROM View3
NATURAL JOIN
  (SELECT DISTINCT district, country FROM customer_dim) AS tmp_cust
GROUP BY country
ORDER BY sum(sum) DESC
LIMIT 1;
