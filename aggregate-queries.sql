--This query shows the total household count for each geographical division in the United States.
SELECT b.DIVISION, COUNT(DISTINCT a.HRHHID) as TotalHouseHold
FROM `Current_Population_Data.PopulationData`  a
FULL OUTER JOIN `Current_Population_Data.GeoDivision`  b
on a.GEDIV=b.GEDIV
GROUP BY b.DIVISION

--This query gives the total household count in this dataset for the United States.
SELECT SUM(tableA.TotalHouseHold)
FROM
(SELECT b.DIVISION, COUNT(DISTINCT a.HRHHID) as TotalHouseHold
FROM `Current_Population_Data.PopulationData`  a
FULL OUTER JOIN `Current_Population_Data.GeoDivision`  b
on a.GEDIV=b.GEDIV
GROUP BY b.DIVISION) tableA


--This query gives the results of the total household count for each race and it is broken down into geographical divisions.
SELECT COUNT(a.HRHHID) as TotalHHCount, b.DIVISION, c.RACE
FROM `Current_Population_Data.PopulationData`  a
FULL OUTER JOIN `Current_Population_Data.GeoDivision`  b
on a.GEDIV=b.GEDIV
FULL OUTER JOIN `Current_Population_Data.EncryptionCode1`  c
on a.PTDTRACE=c.PTDTRACE
WHERE b.DIVISION IS NOT NULL AND c.RACE IS NOT NULL
GROUP BY b.DIVISION, c.Race

--This query shows the total household count for each gender.
SELECT b.GENDER, COUNT(a.HRHHID) as TotalHouseHold
FROM `Current_Population_Data.PopulationData`  a
FULL OUTER JOIN `Current_Population_Data.Gender` b
on a.PESEX=b.PESEX
WHERE b.GENDER IS NOT NULL
GROUP BY b.GENDER

--This query gives the total count of males and female population broken down into geographical divisions in the United States.
SELECT a.GENDER, c.DIVISION, COUNT(b.HRHHID) as TotalHouseHold
FROM `Current_Population_Data.Gender` a
FULL OUTER JOIN `Current_Population_Data.PopulationData`  b
on a.PESEX=b.PESEX
FULL OUTER JOIN `Current_Population_Data.GeoDivision` c
on b.GEDIV=c.GEDIV
WHERE a.GENDER IS NOT NULL and c.DIVISION IS NOT NULL 
GROUP BY a.GENDER, c.DIVISION
