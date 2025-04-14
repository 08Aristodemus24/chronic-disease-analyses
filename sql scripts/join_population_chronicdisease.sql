select * 
from chronicdisease
left join populationsperstateage_00_09
on chronicdisease.YearStart = populationsperstateage_00_09.year and chronicdisease.LocationDesc = populationsperstateage_00_09.state
where state is not null and StratificationCategory1 = 'Gender';

--select distinct(StratificationCategory1)
--from chronicdisease;

--select *
--from chronicdisease;