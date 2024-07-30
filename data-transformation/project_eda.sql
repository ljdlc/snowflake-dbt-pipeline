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

