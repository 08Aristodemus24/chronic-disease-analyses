WITH a AS (
    SELECT DISTINCT(state) FROm us_population
),

b AS (
    SELECT DISTINCT(locationdesc) FROM chronicdisease
)

SELECT * FROM a
LEFT JOIN b
ON a.state = b.locationdesc
WHERE state IS NOT NULL;