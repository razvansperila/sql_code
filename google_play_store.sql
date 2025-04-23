-- creating the table
-- all columns have text as data type because the data needs to be cleaned

create table google_play_store
(
app text,
category text,
rating text,
reviews text,
size text,
installs text,
`type` text,
price text,
`content rating` text,
genres text,
`last updated` text,
`current ver` text,
`android ver` text
);


select *
from google_play_store;

-- data was imported using the table data import wizard

-- creating a staging table with a row number column in case we need an unique identifier for each row
create table google_play_store_staging
select *, row_number() over() as row_num
from google_play_store;


alter table google_play_store_staging
modify row_num int first;

select *
from google_play_store_staging;


-- checking for duplicates
select *
from(
	select *, row_number() over(partition by app) as duplicates
	from google_play_store_staging
	) as sub_duplicates
where duplicates > 1
order by 2
;

delete from google_play_store_staging
where row_num in (
					select row_num
					from(
						select *, row_number() over(partition by app) as duplicates
						from google_play_store_staging
						) as sub_duplicates
where duplicates > 1
				  );


-- cleaning the data and changing the data types

-- category column
select distinct category
from google_play_store_staging
order by 1
;

update google_play_store_staging
set category = null
where category = '1.9';


-- rating column
select distinct rating
from google_play_store_staging
;

update google_play_store_staging
set rating = NULL
where rating = 'NAN';

alter table google_play_store_staging
modify rating double;

select *
from google_play_store_staging
where rating > 5
or rating < 1;

update google_play_store_staging
set rating = NULL
where rating > 5 or rating < 1;

-- reviews column
select distinct reviews
from google_play_store_staging
where reviews regexp '[a-zA-Z]'
;

update google_play_store_staging
set reviews = 3000000
where reviews = '3.0M';

alter table google_play_store_staging
modify reviews int;



-- price column

select distinct price
from google_play_store_staging;

update google_play_store_staging
set price = 0
where price regexp '[a-zA-Z]';

alter table google_play_store_staging
modify price double;


-- genres column
select distinct genres
from google_play_store_staging;

select *
from google_play_store_staging
where genres = '43142';

update google_play_store_staging
set genres = NULL
where genres = '43142';



-- last updated column (was uploaded as the numeric value that excel stores the date)

select `last updated`, date_add('1899-12-30', interval `last updated` day) as dates
from google_play_store_staging;

update google_play_store_staging
set `last updated` = date_add('1899-12-30', interval `last updated` day);


-- current ver column
select distinct `current ver`
from google_play_store_staging;

update google_play_store_staging
set `current ver` = NULL
where `current ver` = 'NaN';



-- android ver column
select distinct `android ver`
from google_play_store_staging;


update google_play_store_staging
set `android ver` = NULL
where `android ver` = 'NaN';



-- exploratory analysis

select *
from google_play_store_staging;



-- discovering free and paid apps with reviews and rating over the average

select app, `type`
from google_play_store_staging
where rating > (select avg(rating) from google_play_store_staging)
and reviews > (select avg(reviews) from google_play_store_staging)
order by 2 desc
;

select category, count(app)
from google_play_store_staging
group by 1
order by 2 desc
;


-- categories by popularity
with cte_over_average as
(
select *
from google_play_store_staging
where rating > (select avg(rating) from google_play_store_staging)
and reviews > (select avg(reviews) from google_play_store_staging)
and year(`last updated`) = (select max(year(`last updated`)) from google_play_store_staging)
)
select category, round(avg(rating),2) as avg_rating,
	format(sum(reviews), 0) as total_reviews,
	concat(format(sum(replace(substring_index(installs, '+', 1), ',', '')), 0), '+') as total_installs
from cte_over_average
group by 1 
order by sum(reviews) desc
;



-- Game category seems to be the most popular



-- checking the number of apps by rating to validate the category choice (a lot of apps with good rating, less apps with low rating)

select count(case when rating < 2 then 1 end) as under_2,
	   count(case when rating > 2 and rating < 3 then 1 end) as between_2_and_3,
       count(case when rating > 3 and rating < 4 then 1 end) as between_3_and_4,
       count(case when rating > 4 then 1 end) as over_4
from google_play_store_staging
where category = 'GAME'
order by 1;



-- rating the app as 'for everyone' seems to bring the best results in terms downloads, reviews and rating
with cte_over_average as
(
select *
from google_play_store_staging
where rating > (select avg(rating) from google_play_store_staging)
and reviews > (select avg(reviews) from google_play_store_staging)
and category = 'GAME'
and year(`last updated`) = (select max(year(`last updated`)) from google_play_store_staging)
)
select `content rating`, round(avg(rating),2) as avg_rating, format(sum(reviews),0) as total_reviews,
	concat(format(sum(replace(substring_index(installs, '+', 1), ',', '')), 0), '+') as total_installs
from cte_over_average
group by 1;


-- creating a temporary table because i'm going to query this output multiple times

create temporary table temp_google_play_proj_catGAME
select *
from google_play_store_staging
where rating > (select avg(rating) from google_play_store_staging)
and reviews > (select avg(reviews) from google_play_store_staging)
and category = 'GAME'
and year(`last updated`) = (select max(year(`last updated`)) from google_play_store_staging);


select *
from temp_google_play_proj_catGAME;


-- there is only 1 paid app(not great stats), so focus should be on free apps 
select type, count(app)
from temp_google_play_proj_catGAME
group by 1;

select *
from temp_google_play_proj_catGAME
where type = 'Paid';


-- action, arcade, casual and racing seem to be the genres that stand out the most

select genres, round(avg(rating),2) as avg_rating, format(sum(reviews),0) as total_reviews,
	concat(format(sum(replace(substring_index(installs, '+', 1), ',', '')), 0), '+') as total_installs
from temp_google_play_proj_catGAME
where type <> 'Paid'
group by 1
order by sum(reviews) desc
;



-- identifying top 10 apps in the 'game' category, filtered by all the findings that resulted from the analysis
with app_cte as
(
select *
from google_play_store_staging
where rating > (select avg(rating) from google_play_store_staging)
and reviews > (select avg(reviews) from google_play_store_staging)
and category = 'GAME'
and year(`last updated`) = (select max(year(`last updated`)) from google_play_store_staging)
)
select regexp_replace(app, '[^\\x00-\\x7F]','') as Top_10_Apps
from app_cte
where lower(genres) in ('action', 'arcade', 'casual', 'racing')
and type = 'Free'
and rating > (select avg(rating) from app_cte)
and reviews > (select avg(reviews) from app_cte)
and lower(`content rating`) like '%everyone%'
order by cast(replace(substring_index(installs, '+', 1), ',', '') as unsigned) desc
limit 10
;
















