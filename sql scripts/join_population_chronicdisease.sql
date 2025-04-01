-- usage: joins the population and chronic disease tables on the
-- state and population year and the location description and 
-- the starting year in these tables respectively

WITH new_us_population AS (
    SELECT year AS pyear, state, population
    FROM us_population
),

new_chronic_disease AS (
    SELECT *
    FROM chronicdisease
)

SELECT population, state, pyear, yearstart, yearend, locationdesc, locationabbr, datavalue, datavalueunit, datavaluetype, question
FROM new_chronic_disease
LEFT JOIN new_us_population
ON new_chronic_disease.locationdesc = new_us_population.state AND new_chronic_disease.yearstart = new_us_population.pyear
WHERE state IS NOT NULL
ORDER BY population DESC
LIMIT 50;

-- select count(*) from new_us_population;