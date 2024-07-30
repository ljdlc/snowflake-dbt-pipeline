-- What is the relationship between cal_dt and d_date_sk?
select cal_dt, d_date_sk from date_dim limit 100;
-- d_date_sk increases with cal_dt

-- When is the earlest catalog sale?
select 
    cal_dt, 
    d_date_sk 
from date_dim 
where d_date_sk = (select min(CS_SOLD_DATE_SK) from catalog_sales);
-- 2021-01-01

-- When is the latest catalog sale?
select 
    cal_dt, 
    d_date_sk 
from date_dim 
where d_date_sk = (select max(CS_SOLD_DATE_SK) from catalog_sales);
-- 2024-05-29

-- When is the earlest web sale?
select 
    cal_dt, 
    d_date_sk 
from date_dim 
where d_date_sk = (select min(WS_SOLD_DATE_SK) from web_sales);
-- 2021-01-02

-- When is the latest web sale?
select 
    cal_dt, 
    d_date_sk 
from date_dim 
where d_date_sk = (select max(WS_SOLD_DATE_SK) from web_sales);
-- 2024-05-29


-- Genearlly, how many catalog sales per day?
select
    d.cal_dt,
    cs.cs_sold_date_sk,
    count(*)
from
    catalog_sales cs
inner join date_dim d
    on cs.cs_sold_date_sk=d.d_date_sk
group by 1,2
order by 1 desc
limit 10;
-- around 400-500 sales per day

-- Genearlly, how many web sales per day?
select
    d.cal_dt,
    ws.ws_sold_date_sk,
    count(*)
from
    web_sales ws
inner join date_dim d
    on ws.ws_sold_date_sk=d.d_date_sk
group by 1,2
order by 1 desc
limit 10;
-- around 200-400 sales per day

-- Verify date_dim and cs_sold_date_sk have corresponding values
select
    fact.cs_sold_date_sk,
    dim.*
from catalog_sales fact
inner join date_dim dim
    on dim.d_date_sk = fact.cs_sold_date_sk
limit 5;
-- cs_sold_date_sk correctly has matching values in date dimension

-- Verify time_dim and cs_sold_time_sk have corresponding values
select
    fact.cs_sold_date_sk,
    dim.*
from catalog_sales fact
inner join time_dim dim
    on dim.t_time_sk = fact.cs_sold_time_sk
limit 5;
-- cs_sold_time_sk correctly has matching values in time dimension

-- Verify connection bewteen catalog sales and customer cs_bill_customer table
select
    distinct fact.cs_bill_customer_sk,
    dim.*
from
    catalog_sales as fact
inner join customer as dim
    on dim.c_customer_sk = fact.cs_bill_customer_sk
limit 5;

-- How often is the inventory table updated?
select
    inv_date_sk,
    inv_item_sk,
    count(*) as count
from inventory
group by 1,2
order by 2,1
limit 20;
-- inv_date_sk increases increases by 7 for the similar inv_item_sk entries so that must mean inventory is updated every 7 days.

-- Double-check the above assumption. For inv_item_sk = 1, how frequently is it being recorded in the inventory table?
select distinct
    date.cal_dt,
    date.wk_num
from date_dim date
inner join inventory inv
    on date.d_date_sk = inv.inv_date_sk
    and inv_item_sk = 1
order by 1,2;
-- Assumption is correct, for inv_item_sk = 1, new entries are added every 7 days/every week

-- For inv_item_sk = 1, how frequently is it being sold by web sales?
select distinct
    date.cal_dt,
    date.wk_num
from web_sales ws
inner join date_dim date
    on date.d_date_sk = ws.ws_sold_date_sk
where ws.ws_item_sk=1
order by 1;
--  There is no specific frequency or trend of sales for item inv_item_sk = 1

-- How many total items are there?
select
    count(distinct i_item_sk) as total_items
from item;
-- 18000 items 

-- How many individual customers?
select
    count(distinct c_customer_sk) as total_customers
from customer;
-- 100,000 customers

-- Add table descriptions
comment on table date_dim is 'This table is used to convert d_date_sk key in other tables to calendar date. This is the date dimension in the source.';

comment on column date_dim.cal_dt is 'Calendar Date';
