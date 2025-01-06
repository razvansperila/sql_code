
create table gym_members_exercise_tracking
(
age int,
gender char(10),
weight_kg double,
height_m double,
max_bpm int,
avg_bpm int,
resting_bpm int,
session_duration_h double,
calories_burned int, 
workout_type char(20),
fat_percentage double,
water_intake_l double,
workout_frequency_days_per_week int,
experience_level int,
BMI double
)
;

create table gym_members_exercise_tracking_2
(
age int,
gender char(10),
weight_kg double,
height_m double,
max_bpm int,
avg_bpm int,
resting_bpm int,
session_duration_h double,
calories_burned int, 
workout_type char(20),
fat_percentage double,
water_intake_l double,
workout_frequency_days_per_week int,
experience_level int,
BMI double
)
;

create table gym_project as
with cte_union_gym as
(
select *
from gym_members_exercise_tracking

union

select *
from gym_members_exercise_tracking_2
)
select *,
row_number() over() as id
from cte_union_gym;


create table gym_proj_staging as
select *
from gym_project;

alter table gym_proj_staging
modify id bigint first;

select *
from gym_proj_staging;

select distinct gender
from gym_proj_staging;

select distinct workout_type
from gym_proj_staging;


select id
from(
	select id, row_number() over(partition by id, age, gender, weight_kg, height_m, max_bpm, avg_bpm, resting_bpm, session_duration_h,calories_burned,workout_type,fat_percentage,water_intake_l,workout_frequency_days_per_week,experience_level,BMI) as ranking
	from gym_proj_staging
	) as doubles
where ranking > 1;

#dataset looks clean, moving to exploratory data analisys

#labeling people based on their BMI and age

select min(age), max(age)
from gym_proj_staging;

create view view_gym_proj_labels as
(
select *,
	case when BMI < 18.5 then 'Underweight'
		when BMI between 18.5 and 24.9 then 'Healthy'
		when BMI between 25 and 29.9 then 'Overweight'
        when BMI >= 30 then 'Obesity'
	end as weight_label,
    case 
    when age between 18 and 29 then '18-29'
    when age between 30 and 39 then '30-39'
    when age between 40 and 49 then '40-49'
    when age between 50 and 59 then '50-59'
    when age >=60 then '60+'
    end as age_label
from gym_proj_staging
);

select *
from view_gym_proj_labels;


#average workouts per week by weight and age labels
select age_label, weight_label, count(*) as number_of_people, avg(workout_frequency_days_per_week) avg_workout_week
from view_gym_proj_labels
where weight_label not like 'Healthy'
group by 1,2
order by 1
;


#percentage overweight and underweight by gender(over entire dataset)
select weight_label, gender, count(*) / (select count(*) from view_gym_proj_labels) * 100 as perc_of_people
from view_gym_proj_labels
where weight_label <> 'Healthy'
group by 1,2
order by 1;



#rank exercise type by weight_label

select *, dense_rank() over(partition by weight_label order by number_of_people desc) as ranking
from(
		select weight_label, workout_type, count(*) as number_of_people
		from view_gym_proj_labels
		where weight_label <> 'Healthy'
		group by 1,2
	) sub_exercise_type
;



select *
from view_gym_proj_labels;


#finding the target audience for customized offers

#count of members by experience_level
select experience_level, count(experience_level) as nr_of_people
from view_gym_proj_labels
group by 1
order by 1;


select age_label, weight_label, gender, count(*) as nr_of_people, avg(workout_frequency_days_per_week) as avg_workouts_weekly
from view_gym_proj_labels
where experience_level = 1
and weight_label <> 'Healthy'
group by 1, 2, 3
order by 1, 2, 3;




#exercise: for every age_label and coreseponding weight_label return the gender and numerical difference where the number of people is higher 
with cte_ex as
(
select age_label, weight_label, gender, count(*) as nr_of_people, avg(workout_frequency_days_per_week) as avg_workouts_weekly
from view_gym_proj_labels
where experience_level = 1
and weight_label <> 'Healthy'
group by 1, 2, 3
order by 1, 2, 3
)
select male.age_label, male.weight_label, abs(male.nr_of_people - female.nr_of_people) as higher_nr_of_people,
	case
		when male.nr_of_people > female.nr_of_people then male.gender
        when female.nr_of_people > male.nr_of_people then female.gender
        else 'Tie'
    end as higher_gender
from (select *
	from cte_ex
	where gender = 'Male'
    ) as male
join
	(select *
    from cte_ex
    where gender = 'Female'
    ) as female
on male.age_label = female.age_label
and male.weight_label = female.weight_label;


#extracting target audience for personalised workout plans
#to prioritize bpm_flag = 1

select distinct workout_type
from view_gym_proj_labels;


with cte_target_audience as
(
select id, age, gender, weight_label, workout_type, 'strength training' as needs, max_bpm, avg_bpm, 220-age as max_bpm_should_have, concat(round((220-age) * 0.50), '-', round((220-age) * 0.85)) as target_bpm
from view_gym_proj_labels
where experience_level = 1
and weight_label = 'Underweight'
and workout_type <> 'Strength'

union

select id, age, gender, weight_label, workout_type, 'cardio/HIIT' as needs, max_bpm, avg_bpm, 220-age as max_bpm_should_have, concat(round((220-age) * 0.50), '-', round((220-age) * 0.85)) as target_bpm
from view_gym_proj_labels
where experience_level = 1
and weight_label = 'Obesity'
and workout_type <> 'Cardio'
and workout_type <> 'HIIT'

union

select id, age, gender, weight_label, workout_type, 'cardio/HIIT' as needs, max_bpm, avg_bpm, 220-age as max_bpm_should_have, concat(round((220-age) * 0.50), '-', round((220-age) * 0.85)) as target_bpm
from view_gym_proj_labels
where experience_level = 1
and weight_label = 'Overweight'
and workout_type <> 'Cardio'
and workout_type <> 'HIIT'
)
select *,
	case when max_bpm - max_bpm_should_have > 10 then 1
		when avg_bpm - substring_index(target_bpm, '-', -1) > 10 then 1
        when avg_bpm - substring_index(target_bpm, '-', 1) < 10 then 1
        else 0
	end as bpm_flag
from cte_target_audience
;


show full tables in exercises
where Table_type = 'VIEW';

show create view view_gym_proj_labels;


drop table gym_members_exercise_tracking;
drop table gym_members_exercise_tracking_2;
drop table gym_project;
rename table gym_proj_staging to gym_members_exercise_tracking_project;


create or replace view view_gym_proj_labels as
(
select *,
	case when BMI < 18.5 then 'Underweight'
		when BMI between 18.5 and 24.9 then 'Healthy'
		when BMI between 25 and 29.9 then 'Overweight'
        when BMI >= 30 then 'Obesity'
	end as weight_label,
    case 
    when age between 18 and 29 then '18-29'
    when age between 30 and 39 then '30-39'
    when age between 40 and 49 then '40-49'
    when age between 50 and 59 then '50-59'
    when age >=60 then '60+'
    end as age_label
from gym_members_exercise_tracking_project
);

select *
from view_gym_proj_labels;