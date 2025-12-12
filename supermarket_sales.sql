
-- cleaning the dataset


select *
from supermarket_sales;

-- select distinct values from text columns to see if standardization is needed
select distinct branch
from supermarket_sales;

select distinct `Product line`
from supermarket_sales;

-- verify numeric columns for impossible values (out of the boundaries)

select row_number() over() as row_num
from supermarket_sales
where cost < 0
or `unit price` < 0
or quantity < 0
or rating < 0;


select
	min(`date`), max(`date`),
    min(cost), max(cost),
    min(`unit price`), max(`unit price`),
    min(quantity), max(quantity),
    min(rating), max(rating)
from supermarket_sales;


-- dataset looks clean, moving to exploratory analysis


-- revenue and profit + mom growth by months
select 
	month(`date`) as months,
    format(sum(`unit price` * quantity),0) as revenue,
    format(sum((`unit price` - cost) * quantity),0) as profit,
    round(sum((`unit price` - cost) * quantity) / sum(cost * quantity) - 1, 2) as profit_perc,
    round(sum(`unit price` * quantity) / lag(sum(`unit price` * quantity)) over(order by month(`date`)) - 1, 2)  as mom_growth
from supermarket_sales
group by 1
order by 1
;

select 
	month(`date`) as months,
    format(sum(`unit price` * quantity),0) as revenue,
    format(sum((`unit price` - cost) * quantity),0) as profit,
    round(sum((`unit price` - cost) * quantity) / sum(cost * quantity), 2) as profit_perc,
    round(sum((`unit price` - cost) * quantity) / lag(sum((`unit price`-cost) * quantity)) over(order by month(`date`)), 2) as mom_growth_profit
from supermarket_sales
group by 1
order by 1
;


-- yearly order volume and profit by product category

select `Product line`,
    sum(quantity) as total_quant,
    sum((`unit price` - cost) * quantity) as profit,
    round(sum((`unit price` - cost) * quantity) / (select sum((`unit price` - cost) * quantity) from supermarket_sales) * 100,1) as profit_perc_from_total,
    round(sum((`unit price` - cost) * quantity) / sum(quantity),1) weighted_asp
from supermarket_sales
group by 1
order by 3 desc;


-- best selling product categories by branch

select `product line`,
	sum(case when branch = 'Brooklyn' then (`unit price` - cost) * quantity end) as Brooklyn,
    sum(case when branch = 'Manhattan' then (`unit price` - cost) * quantity end) as Manhattan,
    sum(case when branch = 'Queens' then (`unit price` - cost) * quantity end) as Queens,
    sum((`unit price` - cost) * quantity) as total_product_line
from supermarket_sales
group by 1

union

select 'Total' as `product line`,
	sum(case when branch = 'Brooklyn' then (`unit price` - cost) * quantity end) as Brooklyn,
	sum(case when branch = 'Manhattan' then (`unit price` - cost) * quantity end) as Manhattan,
	sum(case when branch = 'Queens' then (`unit price` - cost) * quantity end) as Queens,
	sum((`unit price` - cost) * quantity) as total_product_line
from supermarket_sales
;


-- favorite payment method by customer type

select `customer type`,
	payment,
	count(*) as payments
from supermarket_sales
group by 1, 2
order by 1, 2 
;

select payment,
count(case when branch = 'Brooklyn' then payment end) as Brooklyn,
count(case when branch = 'Queens' then payment end) as Queens,
count(case when branch = 'Manhattan' then payment end) as Manhattan
from supermarket_sales
group by 1
;


select *
from supermarket_sales;


-- what gender is predominant in purchases by product line


select `product line`,
	count(case when gender = 'Female' then 1 end) as female_buyers,
	count(case when gender = 'Male' then 1 end) as male_buyers,
	sum(case when gender = 'Female' then `unit price` * quantity end) as revenue_generated_by_female,
	sum(case when gender = 'Male' then `unit price` * quantity end) as revenue_generated_by_male
from supermarket_sales
group by 1

union

select 'total' as `product line`,
	count(case when gender = 'Female' then 1 end) as female_buyers,
	count(case when gender = 'Male' then 1 end) as male_buyers,
	sum(case when gender = 'Female' then `unit price` * quantity end) as revenue_generated_by_female,
	sum(case when gender = 'Male' then `unit price` * quantity end) as revenue_generated_by_male
from supermarket_sales
;



-- is there any week of the month where customer buy more ?

select 
	case 
		when day(`date`) < 11 then 'period_1_10'
        when day(`date`) between 11 and 20 then 'period_11_20'
        when day(`date`) > 20 then 'period_21_31'
    end as days,
	sum(`unit price` * quantity) as revenue
from supermarket_sales
group by 1
;



-- average rating by branch, slice by product line
select `product line`,
	avg(case when branch = 'Brooklyn' then rating end) as Brooklyn,
    avg(case when branch = 'Queens' then rating end) as Queens,
    avg(case when branch = 'Manhattan' then rating end) as Manhattan
from supermarket_sales
group by 1
;

-- rating by gender and cust type
select `customer type`,
	round(avg(case when gender = 'Male' then rating end),2) as male,
    round(avg(case when gender = 'Female' then rating end),2) as female
from supermarket_sales
group by 1
;






-- insights
-- there is a significant decrease in revenue and profit between months 5 and 8, and the highest revenue increase recorded in month 9. The lowest values ​​are recorded during the summer.
-- the profit is distributed pretty evenly between all the product categories, highest generating 21% of the total revenue and smallest 11%
-- branches have similar performances for total revenue(3% difference between best and worst) but when sliced by product category, every branch has different best sellers and areas of improvement
-- customers doesn't seem to have a preferred payment type, either members or regular customers. same thing if we slice by branch instead of customer type
-- the number of female customers is 6% higher and the revenue generated by female customers is 8% higher than male
-- people tend to spend more money on the first period of the month(day 1 to 10).
-- ratings are similar, there are specific product categories in specifi branches that may be subject for improvement
