-- usage: joins the population and chronic disease tables on the
-- state and population year and the location description and 
-- the starting year in these tables respectively

WITH a AS (
    SELECT population, state, year AS pyear, yearstart, yearend, locationdesc, locationabbr, datavalue, datavalueunit, datavaluetype, question
    FROM chronicdisease
    LEFT JOIN statepopulation
    ON chronicdisease.locationdesc = statepopulation.state AND chronicdisease.yearstart = statepopulation.year
    WHERE state IS NOT NULL
    ORDER BY population DESC
)

SELECT * FROM a
WHERE pyear = 2021
LIMIT 50;