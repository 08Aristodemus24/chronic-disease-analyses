-- selects all the common states in both population
-- and chronic disease tables

WITH a AS (
    SELECT DISTINCT(state) FROM statepopulation
),

b AS (
    SELECT DISTINCT(locationdesc) FROM chronicdisease
)

SELECT * FROM a
LEFT JOIN b
ON a.state = b.locationdesc
WHERE state IS NOT NULL;