create table youtube_channel_analysis
(
source_name text,
`date` date,
dislikes int,
likes int,
subscribers_lost int,
subscribers_gained int,
videos_published int,
impressions int,
views int,
watch_time_hours double,
average_view_duration text,
estimated_revenue_USD double
);

select *
from youtube_channel_analysis;

create table youtube_proj
select *, row_number() over() as row_num
from youtube_channel_analysis;

select *
from youtube_proj;


alter table youtube_proj
drop column source_name;

alter table youtube_proj
modify row_num bigint first;

alter table youtube_proj
modify row_num int;

select row_num
from (
		select *,
        row_number() over(partition by `date`, dislikes, likes, subscribers_lost, subscribers_gained, videos_published, impressions, views, watch_time_hours, average_view_duration_sec, estimated_revenue_USD) as doubles
		from youtube_proj
	)sub_doubles
where doubles > 1;


delete from youtube_proj
where row_num in (
					select row_num
					from (
							select *, row_number() over(partition by `date`, dislikes, likes, subscribers_lost, subscribers_gained, videos_published, impressions, views, watch_time_hours, average_view_duration_sec, estimated_revenue_USD) as doubles
							from youtube_proj
						)sub_doubles
					where doubles > 1
				);


select average_view_duration, substring(average_view_duration, 3, 5),
	case when substring_index(substring(average_view_duration, 3, 5), ':', 1) = 01 then 60 else 0 end + substring_index(substring(average_view_duration, 3, 5), ':', -1)
from youtube_proj;

update youtube_proj
set average_view_duration =
	case when substring_index(substring(average_view_duration, 3, 5), ':', 1) = 01 then 60 else 0 end + substring_index(substring(average_view_duration, 3, 5), ':', -1);

alter table youtube_proj
rename column average_view_duration to average_view_duration_sec;

alter table youtube_proj
modify average_view_duration_sec int;

#dataset required minimal changes, moving to data analysis

select min(date), max(date)
from youtube_proj;

select *
from youtube_proj;

select `date`, views, impressions, impressions/views, estimated_revenue_USD
from youtube_proj
order by 2 desc
;


#analyzing views and revenue trends over time

select year(`date`), month(`date`), round(sum(views),2) as views, round(sum(estimated_revenue_usd),2) as revenue,
round(avg(estimated_revenue_usd / views),6) as revenue_per_view
from youtube_proj
where year(`date`) <> 2020
group by 1, 2
order by 1, 2
;

select year(`date`), round(sum(views),2) as views, round(sum(impressions),2) as impressions, round(avg(impressions/views),2) as avg_impression_views, round(sum(estimated_revenue_usd),2) as revenue,
round(avg(estimated_revenue_usd / views),6) as revenue_per_view
from youtube_proj
where year(`date`) <> 2020
and year(`date`) <> 2025
and estimated_revenue_usd <> 0
group by 1
order by 1
;

select month(`date`), round(sum(views),2) as views, round(sum(impressions),2) as impressions, round(avg(impressions/views),2) as avg_impression_views, round(sum(estimated_revenue_usd),2) as revenue,
round(avg(estimated_revenue_usd / views),6) as revenue_per_view
from youtube_proj
where year(`date`) <> 2020
and year(`date`) <> 2025
and estimated_revenue_usd <> 0
group by 1
order by 1
;


select day(`date`), round(sum(views),2) as views, round(sum(impressions),2) as impressions, round(avg(impressions/views),2) as avg_impression_views, round(sum(estimated_revenue_usd),2) as revenue,
round(avg(estimated_revenue_usd / views),6) as revenue_per_view
from youtube_proj
where year(`date`) <> 2020
and year(`date`) <> 2025
and estimated_revenue_usd <> 0
group by 1
order by 1
;


#checking upload frequency

with cte_uploads as
(
select year(`date`) as years, coalesce(videos_published,0) as uploads_per_day, count(`date`) as nr_of_days
from youtube_proj
where year(`date`) not in (2020, 2025)
group by 1, 2
order by 1, 3 desc
)
select *, sum(case when uploads_per_day <> 0 then uploads_per_day * nr_of_days end) over(partition by years) as total_uploads_year
from cte_uploads
;



with cte_uploads as
(
select year(`date`) as years, month(`date`) as months, coalesce(videos_published,0) as uploads_per_day, count(`date`) as nr_of_days
from youtube_proj
where year(`date`) not in (2020, 2025)
group by 1, 2, 3
)
select *, sum(case when uploads_per_day <> 0 then uploads_per_day * nr_of_days end) over(partition by months, years) as total_uploads_month,
		  sum(case when uploads_per_day <> 0 then uploads_per_day * nr_of_days end) over(partition by years) as total_uploads_year
from cte_uploads
order by 1,2
;


select *
from youtube_proj;

#creating a dynamic monthly report

create view view_yt_monthly_report as
(
select month(`date`) as `month`, 
	sum(videos_published) as uploads,
	sum(likes) as likes,
	sum(dislikes) as dislikes,
    sum(likes) - sum(dislikes) as likes_vs_dislikes,
    sum(subscribers_gained) as subs_gained,
    sum(subscribers_lost) as subs_lost,
    sum(subscribers_gained) - sum(subscribers_lost) as subs_difference,
    sum(impressions) as impressions,
    sum(views) as views,
    round(sum(impressions) / sum(views),2) impressions_per_view,
    round(sum(watch_time_hours),2) as watch_time_h,
    round(avg(average_view_duration_sec),2) avg_view_duration_sec,
    round(sum(estimated_revenue_USD),2) as revenue,
    round(sum(estimated_revenue_USD) / sum(views),6) as revenue_per_view,
    round(sum(estimated_revenue_USD) / sum(watch_time_hours),2) as revenue_per_hour_watched
from youtube_proj
where month(`date`) = if(month(now()) = 1, 12, month(now()) -1)
and year(`date`) = case
						when month(now()) = 1 then year(now()) - 1
                        else year(now())
					end
group by 1
);


select *
from view_yt_monthly_report;


#using this report to compare months

select month(`date`) as `month`, 
	sum(videos_published) as uploads,
	sum(likes) as likes,
	sum(dislikes) as dislikes,
    sum(likes) - sum(dislikes) as likes_vs_dislikes,
    sum(subscribers_gained) as subs_gained,
    sum(subscribers_lost) as subs_lost,
    sum(subscribers_gained) - sum(subscribers_lost) as subs_difference,
    sum(impressions) as impressions,
    sum(views) as views,
    round(sum(impressions) / sum(views),2) impressions_per_view,
    round(sum(watch_time_hours),2) as watch_time_h,
    round(avg(average_view_duration_sec),2) avg_view_duration_sec,
    round(sum(estimated_revenue_USD),2) as revenue,
    round(sum(estimated_revenue_USD) / sum(views),6) as revenue_per_view,
    round(sum(estimated_revenue_USD) / sum(watch_time_hours),2) as revenue_per_hour_watched
from youtube_proj
where year(`date`) = 2024
group by 1
order by 1;

#finding months with higher volume of views than previous month

select `month`
from(
		with cte_views_comparation as
		(
		select month(`date`) as `month`, 
			sum(videos_published) as uploads,
			sum(likes) as likes,
			sum(dislikes) as dislikes,
			sum(likes) - sum(dislikes) as likes_vs_dislikes,
			sum(subscribers_gained) as subs_gained,
			sum(subscribers_lost) as subs_lost,
			sum(subscribers_gained) - sum(subscribers_lost) as subs_difference,
			sum(impressions) as impressions,
			sum(views) as views,
			round(sum(impressions) / sum(views),2) impressions_per_view,
			round(sum(watch_time_hours),2) as watch_time_h,
			round(avg(average_view_duration_sec),2) avg_view_duration_sec,
			round(sum(estimated_revenue_USD),2) as revenue,
			round(sum(estimated_revenue_USD) / sum(views),6) as revenue_per_view,
			round(sum(estimated_revenue_USD) / sum(watch_time_hours),2) as revenue_per_hour_watched
		from youtube_proj
		where year(`date`) = 2024
		group by 1
		order by 1
		)
		select `month`, views, lag(views) over(order by month) as prev_month
		from cte_views_comparation
	)sub_views_comp
where views > prev_month;


#comparing months over the years

select year(`date`) as `year`,
	month(`date`) as `month`,
	coalesce(sum(videos_published),0) as uploads,
    sum(impressions) as impressions,
    sum(views) as views,
    coalesce(round(sum(estimated_revenue_usd),2),0) as revenue
from youtube_proj
where year(`date`) <> 2020
and year(`date`) <> 2025
group by 1, 2
order by 2, 1;


drop table youtube_channel_analysis;
rename table youtube_proj to youtube_channel_analysis;

create or replace view view_yt_monthly_report as
(
select month(`date`) as `month`, 
	sum(videos_published) as uploads,
	sum(likes) as likes,
	sum(dislikes) as dislikes,
    sum(likes) - sum(dislikes) as likes_vs_dislikes,
    sum(subscribers_gained) as subs_gained,
    sum(subscribers_lost) as subs_lost,
    sum(subscribers_gained) - sum(subscribers_lost) as subs_difference,
    sum(impressions) as impressions,
    sum(views) as views,
    round(sum(impressions) / sum(views),2) impressions_per_view,
    round(sum(watch_time_hours),2) as watch_time_h,
    round(avg(average_view_duration_sec),2) avg_view_duration_sec,
    round(sum(estimated_revenue_USD),2) as revenue,
    round(sum(estimated_revenue_USD) / sum(views),6) as revenue_per_view,
    round(sum(estimated_revenue_USD) / sum(watch_time_hours),2) as revenue_per_hour_watched
from youtube_channel_analysis
where month(`date`) = if(month(now()) = 1, 12, month(now()) -1)
and year(`date`) = case
						when month(now()) = 1 then year(now()) - 1
                        else year(now())
					end
group by 1
);






