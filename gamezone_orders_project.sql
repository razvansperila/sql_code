-- got 2 tables: orders and regions
-- first i'm going to clean and standardize the data within both tables then analyze it

-- T1 starting with regions table

-- checking for duplicates
select country_code, count(country_code)
from gamezone_regions
group by 1
order by 2 desc;

select distinct region
from gamezone_regions
;

-- standardizing North America to NA
select *
from gamezone_regions
where region in ('NA', 'North America');

update gamezone_regions
set region = 'NA'
where region = 'North America';

-- filling missing or incorrect values
select *
from gamezone_regions
where region in ('', 'X.x');

update gamezone_regions
set region = 'EMEA'
where region = '';

update gamezone_regions
set region = 'APAC'
where region = 'X.x';


-- T2 moving to orders table

select *
from gamezone_orders
limit 10000;

-- checking for duplicates

select *
from(
		select *, row_number() over(partition by user_id, order_id, purchase_ts, ship_ts, product_name, product_id, usd_price, purchase_platform, marketing_channel, account_creation_method, country_code) as row_num
		from gamezone_orders
	) sub_orders
where row_num > 1;

-- taking a look at how these duplicates look

select *
from gamezone_orders
where user_id in(
				select user_id
				from(
						select *, row_number() over(partition by user_id, order_id, purchase_ts, ship_ts, product_name, product_id, usd_price, purchase_platform, marketing_channel, account_creation_method, country_code) as row_num
						from gamezone_orders
					) sub_orders
where row_num > 1
				);

-- because there's no quantity column, if someone bought 2 identical items at the same time, there would be 2 lines recorded since the user_id, order_id and product_id would be the same, so i won't remove duplicates


-- C1 purchase date column
-- there were some timestamp values which i removed (10 records)
select purchase_ts, replace(substring_index(purchase_ts, ' ', 1), '-', '/') as date_standardized
from gamezone_orders
where purchase_ts like '%:%'
;

update gamezone_orders
set purchase_ts = replace(substring_index(purchase_ts, ' ', 1), '-', '/')
where purchase_ts like '%:%';

--  changing date format to mysql's standard date format, and the column's datatype to date
select purchase_ts, str_to_date(purchase_ts, '%m/%d/%Y')
from gamezone_orders
;

update gamezone_orders
set purchase_ts = str_to_date(purchase_ts, '%m/%d/%Y')
;

alter table gamezone_orders
modify PURCHASE_TS date;

-- encountered an issue, some value in the date column had 2 spaces instead of a date value
select distinct purchase_ts, length(purchase_ts)
from gamezone_orders
order by 2
;

select *
from gamezone_orders
where purchase_ts = '  ';

update gamezone_orders
set purchase_ts = NULL
where purchase_ts = '  ';

-- C2 shipping date column
-- column had all the values filled correctly so I just changed to mysql's format and column's datatype to date
select distinct ship_ts, length(ship_ts) as len, max(length(ship_ts)) over() as max_len, min(length(ship_ts)) over() as min_len
from gamezone_orders;

select ship_ts, str_to_date(ship_ts, '%m/%d/%Y')
from gamezone_orders;

update gamezone_orders
set ship_ts = str_to_date(ship_ts, '%m/%d/%Y');

alter table gamezone_orders
modify SHIP_TS date;

-- checkpoint

create table gamezone_orders_2
select *
from gamezone_orders;


select *
from gamezone_orders_2;

-- just noticed there are multiple product_ids recorded with the same product_name
select product_name, product_id, count(product_id)
from gamezone_orders_2
group by 1, 2
order by 1, 2;

-- 5 times more product_ids than product_names
select count(distinct product_name), count(distinct product_id)
from gamezone_orders_2;


-- the price can also be different for the same product_id
select distinct product_name, product_id, usd_price
from gamezone_orders_2
order by 1;


-- C3 product name column
-- minor standardization changes, I chose the variant with more records
select distinct product_name
from gamezone_orders_2
order by 1;

select count(case when product_name = '27in 4K gaming monitor' then 1 end) as `27in`,
	count(case when product_name = '27inches 4k gaming monitor' then 1 end) as `27inches`
from gamezone_orders_2;

update gamezone_orders_2
set product_name = '27in 4K gaming monitor'
where product_name = '27inches 4k gaming monitor';


-- C4 price column
-- checking the column for non numerical values or blanks to convert into nulls and change the datatype to double
select distinct usd_price
from gamezone_orders_2
where usd_price not regexp '^[0-9]+(\.[0-9]+)?$';

select usd_price
from gamezone_orders_2
where usd_price = '';

update gamezone_orders_2
set usd_price = 0
where usd_price = '';

alter table gamezone_orders_2
modify USD_PRICE double;

select *
from gamezone_orders_2;

-- for columns marketing_channel and account_creation_method I changed blanks to nulls

update gamezone_orders_2
set marketing_channel = NULL 
WHERE marketing_channel = ''
;

update gamezone_orders_2
set ACCOUNT_CREATION_METHOD = NULL 
WHERE ACCOUNT_CREATION_METHOD = ''
;

-- checkpoint 2

create table gamezone_orders_3
select *
from gamezone_orders_2;


-- moving to data analysis

select *
from gamezone_orders_3;

-- there are orders where the shipping date is before the order date
select *
from gamezone_orders_3
where ship_ts < purchase_ts
;


-- looking for orders with price <= 0
select *
from gamezone_orders_3
where usd_price <= 0;

-- creating a view to filter out the data I don't want to include in the analysis and add the region
-- considering the analysis will mostly be focused on regions, I also excluded orders where the country code was missing

create or replace view view_gamezone as
select user_id, order_id, purchase_ts, ship_ts, product_name, product_id, usd_price, purchase_platform, marketing_channel, account_creation_method, o.country_code, region
from gamezone_orders_3 o
join gamezone_regions r
	on o.country_code = r.country_code
where ship_ts >= purchase_ts
and usd_price > 0
and datediff(ship_ts, purchase_ts) < 30;


select *
from view_gamezone;

-- products by popularity/ quantity sold

select product_name, count(product_name) as quant
from view_gamezone
group by 1
order by 2 desc;


-- products revenue by region with totals (pivoted rows to columns for better visualization)

with cte_revenue_by_pord_region as
(
select product_name,
	round(sum(case when region = 'APAC' then usd_price else 0 end),0) as APCA,
    round(sum(case when region = 'EMEA' then usd_price else 0 end),0) as EMEA,
    round(sum(case when region = 'LATAM' then usd_price else 0 end),0) as LATAM,
    round(sum(case when region = 'NA' then usd_price else 0 end),0) as NA,
    round(sum(usd_price),0) as total_revenue_by_prod,
    1 as sort_order
from view_gamezone
group by 1

union

select 'Total by Region' as product_name,
		round(sum(case when region = 'APAC' then usd_price else 0 end),0) as APCA,
    round(sum(case when region = 'EMEA' then usd_price else 0 end),0) as EMEA,
    round(sum(case when region = 'LATAM' then usd_price else 0 end),0) as LATAM,
    round(sum(case when region = 'NA' then usd_price else 0 end),0) as NA,
    round(sum(usd_price),0) as total_revenue_by_prod,
    2 as sort_order
    from view_gamezone
)
select product_name,
	format(apca,0) as APCA,
    format(emea,0)as EMEA,
    format(latam,0) as LATAM,
    format(na,0) as NA,
    format(total_revenue_by_prod,0) as Total_Revenue_Product
from cte_revenue_by_pord_region
order by sort_order, total_revenue_by_prod desc
;


-- average delivery time in days by country and region

with cte_avg_by_country as
(
select region, country_code, round(avg(datediff(ship_ts,purchase_ts)),1) avg_shipping_time_days_country
from view_gamezone
group by 1, 2
),
cte_avg_by_region as
(
select region, round(avg(datediff(ship_ts,purchase_ts)),1) as avg_shipping_time_days_region
from view_gamezone
group by 1
)
select c.region, c.country_code, c.avg_shipping_time_days_country, r.avg_shipping_time_days_region
from cte_avg_by_country c
join cte_avg_by_region r
	on c.region = r.region
-- where avg_shipping_time_days_country > avg_shipping_time_days_region - to find countries with higher avg delivery time than the avg in their region
    order by 1, 2
;


-- looking at trends in revenue over the years

select max(purchase_ts), min(purchase_ts), max(ship_ts), min(ship_ts)
from view_gamezone;

select *
from view_gamezone
where datediff(ship_ts, purchase_ts) > 30;
;

-- years 2019 and 2020 are full, 2021 has data for the first 2 months
-- it seems there are 2 orders which took over 1 year deliver, returning to view to filter them out using - where datediff(ship_ts, purchase_ts) < 30;

with cte_yoy_trend as
(
select region, product_name,
	format(sum(case when year(purchase_ts) = 2019 then usd_price else 0 end),0) as '2019',
    format(sum(case when year(purchase_ts) = 2020 then usd_price else 0 end),0) as '2020',
    format(sum(case when year(purchase_ts) = 2021 then usd_price else 0 end),0) as '2021'
from view_gamezone
group by 1, 2
order by 1, 2
)
select region, product_name, `2019`, `2020`, format((`2020`/`2019` - 1) * 100,2) as YoY_growth
from cte_yoy_trend
;


-- favorite purchase platform in every region

select region, purchase_platform, round(count(purchase_platform) / sum(count(purchase_platform)) over(partition by region) * 100,2) as `%_orders`
from view_gamezone
group by 1, 2
order by 1, 3 desc;


-- which marketing channel drives the most revenue

select marketing_channel, round(sum(usd_price) / (select sum(usd_price) from view_gamezone) * 100,2) as `%_mk_channel`
from view_gamezone
group by 1;


-- by region 

with cte_mk_revenue as 
(
select region, marketing_channel, sum(usd_price) as revenue
from view_gamezone
group by 1, 2
order by 1
),
cte_revenue_region as 
(
select region, sum(usd_price) as revenue_region
from view_gamezone
group by 1
)
select marketing_channel, 
	round(sum(case when mk.region = 'APAC' then revenue / revenue_region * 100 end),2) as APAC,
    round(sum(case when mk.region = 'EMEA' then revenue / revenue_region * 100 end),2) as EMEA,
    round(sum(case when mk.region = 'NA' then revenue / revenue_region * 100 end),2) as NA,
    round(sum(case when mk.region = 'LATAM' then revenue / revenue_region * 100 end),2) as LATAM
from cte_mk_revenue mk
join cte_revenue_region reg
	on mk.region = reg.region
group by 1
order by 1;


-- favorite device used

select account_creation_method, count(*) as cnt
from view_gamezone
group by 1
order by 2 desc
;

select region, account_creation_method, count(*) as cnt
from view_gamezone
group by 1, 2
order by 1, 3 desc
;



-- findings:
-- when it comes to product popularity we have 3 products leading in quantity sold but if we look at revenue generated(either in total or by region), other products which sell less but cost more are at the top of the list
-- average shipping time by region is the same for all regions, but there are countries within every region that exceed region's average
-- looking at year over year growth, every product sold in every region experienced an increase in revenue generated ranging from 25% to 484%
-- favorite purchase platform for all the regions is website
-- 'direct' marketing channel drives the most revenue in every region
-- 'desktop' is the most used device (column account_created_method)












