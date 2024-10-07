with first_year as
(
select distinct LHA as FY_LHA, STUDY_ID as FY_STUDY_ID
from BC_Stat_CLR_EXT_20230525
),

second_year as
(
select distinct LHA as SY_LHA, STUDY_ID as SY_STUDY_ID
from BC_Stat_Population_Estimates_20240527
where end_date>='2024-04-01' or end_date is null
),

total_pop as
(
select LHA as SY_LHA,
count(distinct(STUDY_ID)) as SY_STUDY_ID
from BC_Stat_Population_Estimates_20240527
where end_date>='2024-04-01' or end_date is null
group by LHA
),

first_pop as
(
select LHA as SY_LHA,
count(distinct(STUDY_ID)) as FY_STUDY_ID
from BC_Stat_CLR_EXT_20230525
--where end_date>='2023-07-01' or end_date is null
group by LHA
),

all_movements as
(
select
FY_LHA,SY_LHA,
case

when FY_LHA is not null and SY_LHA is null then 'EXITED'
when FY_LHA is null and SY_LHA is not null then 'ENTERED'
when FY_LHA is null and SY_LHA is null then 'EXISTING'
when FY_LHA<>SY_LHA then 'MOVED'
when FY_LHA=SY_LHA then 'EXISTING'
else 'ERROR' end as STATUS,
count (distinct(FY_STUDY_ID)) as FY_STUDY_ID,
count (distinct(SY_STUDY_ID)) as SY_STUDY_ID
from first_year full outer join second_year on FY_STUDY_ID=SY_STUDY_ID
group by FY_LHA,SY_LHA,
case
--when FY_STUDY_ID is null then 'ENTERED'
--when SY_STUDY_ID is null then 'EXITED'
when FY_LHA is not null and SY_LHA is null then 'EXITED'
when FY_LHA is null and SY_LHA is not null then 'ENTERED'
when FY_LHA is null and SY_LHA is null then 'EXISTING'
when FY_LHA<>SY_LHA then 'MOVED'
when FY_LHA=SY_LHA then 'EXISTING'
else 'ERROR' end
),

entered as
(
select * from all_movements
where status= 'ENTERED'
),

exited as
(
select * from all_movements
where status= 'EXITED'
),

moved as
(
select * from all_movements
where status= 'MOVED'
)

--all movements
--select * from all_movements;

--final table of immigration inflows and outflows
select
b.FY_LHA,
--a.status,b.status,
a. SY_STUDY_ID as entered,
b.FY_STUDY_ID as exited,
(a. SY_STUDY_ID-b.FY_STUDY_ID) as net_growth,
d.FY_STUDY_ID as begin_pop,
c.SY_STUDY_ID as end_pop
from entered a
join exited b on b.FY_LHA = a.SY_LHA
join total_pop c on c.SY_LHA = a.SY_LHA
join first_pop d on d.SY_LHA = a.SY_LHA
order by b.FY_LHA
