select *
from project;

rename table project to bike_project;

drop table if exists bike_project_staging;

CREATE TABLE `bike_project_staging` (
  `ID` int not null,
  `Marital Status` text,
  `Gender` text,
  `Income` text,
  `Children` int DEFAULT NULL,
  `Education` text,
  `Occupation` text,
  `Home Owner` text,
  `Cars` int DEFAULT NULL,
  `Commute Distance` text,
  `Region` text,
  `Age` int DEFAULT NULL,
  `Purchased Bike` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into bike_project_staging
select *,
row_number() over(partition by id) as row_num
from bike_project;


#data cleaning

select *
from bike_project_staging;


select *
from(
	select *,
	row_number() over(partition by id) as row_num
	from bike_project_staging
    ) bike_sub
where row_num > 1
order by id;

select *
from bike_project_staging
where id in(select id
			from(
				select *,
				row_number() over(partition by id) as row_num
				from bike_project_staging
				) bike_sub
			where row_num > 1)
order by id;

delete from bike_project_staging
where row_num > 1;



select id, count(id)
from bike_project_staging
group by 1
having count(id) > 1;


select *
from bike_project_staging;


select distinct `purchased bike`
from bike_project_staging;

alter table bike_project_staging
drop column row_num;

alter table bike_project_staging
add column `Currency` text;

update bike_project_staging
set currency = 'USD';

alter table bike_project_staging
modify column currency text after income;

select income,
	concat(
			substring_index(substring_index(income, '$', -1), ',', 1),
			substring_index(substring_index(income, '$', -1), ',', -1)
		  )
from bike_project_staging;

update bike_project_staging
set income = concat(
					substring_index(substring_index(income, '$', -1), ',', 1),
					substring_index(substring_index(income, '$', -1), ',', -1)
				   );
                   
alter table bike_project_staging
modify column income double;



select *
from bike_project_staging
;

create table bike_project_cleaned
select *
from bike_project_staging;


select *
from bike_project_cleaned;

#EDA

#avg vs max income by region

select region, round(avg(income)) as avg_income, max(income) as max_income, min(income) as min_income, (max(income) + min(income)) / 2 as median_income, currency
from bike_project_cleaned
group by region, currency;


# percentage of bikes purchased by categories of age

select min(age), max(age)
from bike_project_cleaned;


select *
from bike_project_cleaned;


select
	case
		when age between 25 and 40 then '25-40'
        when age between 41 and 60 then '41-60'
        when age between 61 and 90 then '61-90'
    end as people_by_age,
    round((count(case when `Purchased Bike`='Yes' then 1 end) / count(*)) * 100, 2) as perc_purchased_bike
from bike_project_cleaned
group by 1;




select region, occupation, education, count(*) count_of_emp, round(avg(income)) as avg_income
from bike_project_cleaned
group by 1, 2, 3
order by 1,2, 4;



#percentage of home owners and avg number of cars by age groups


select
	case
		when age between 25 and 40 then '25-40'
        when age between 41 and 60 then '41-60'
        when age between 61 and 90 then '61-90'
    end as people_by_age,
    round(count(case when `home owner` = 'Yes' then 1 end) / count(*) * 100, 2) as perc_has_home,
    round(count(case when `home owner` = 'No' then 1 end) / count(*) * 100, 2) as perc_doesnt_have_home,
    round(avg(cars),2) as avg_number_of_cars
from bike_project_cleaned
group by 1;


#income by gender and region

select region, gender, round(avg(income),2) as avg_income
from bike_project_cleaned
group by 1, 2
order by 1, 2;




select *
from bike_project_cleaned;


# percentage of people who use bikes vs their income by occupation 

select *
from(
	select occupation,
		round(count(case when `Purchased Bike` = 'Yes' then 1 end) / count(*) * 100, 2) as perc_purchased_bike
	from bike_project_cleaned
	group by occupation
	) sub_bike
join(
	select occupation, round(avg(income),2) as avg_income
	from bike_project_cleaned
	group by 1
    ) sub_income
using(occupation)
order by 2 desc;



#percentage of people who bought a bike by the commute distance

select region, `commute distance`, perc_purchased_bike
from(
		select region,
			`commute distance`,
            round(count(case when `purchased bike`='Yes' then 1 end) / count(*) * 100, 2) as perc_purchased_bike,
            cast(substring_index(`commute distance`, ' ', 1) as double) as `order`
		from bike_project_cleaned
		group by 1, 2
		order by 1, 4
	)sub_bike

