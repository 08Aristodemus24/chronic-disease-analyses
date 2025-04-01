-- usage: updates the data value unit column with per 
-- 100,000 string and replaces it instead with cases
-- per 100,000 as it is virtually the same and doing
-- otherwise would be redundant

UPDATE chronicdisease SET datavalueunit = 'cases per 100,000'
WHERE datavalueunit LIKE 'per%100,000';