WITH new_us_population AS (
    SELECT year AS pyear, state, population
    FROM us_population
),

new_chronic_disease AS (
    SELECT *
    FROM chronicdisease
)

SELECT population, state, pyear, locationdesc AS locationdesc2, datavalue, datavalueunit, datavaluetype, question
FROM new_chronic_disease
LEFT JOIN new_us_population
ON new_chronic_disease.locationdesc = new_us_population.state
ORDER BY population
LIMIT 50;

-- select count(*) from new_us_population;