--This query shows the total household count for each race in each geographical division in the United States.
SELECT a.RACE, c.DIVISION, COUNT(b.HRHHID) as TotalHouseHold
FROM `Current_Population_Data.EncryptionCode1`  a
FULL OUTER JOIN `Current_Population_Data.PopulationData`  b
on a.PTDTRACE=b.PTDTRACE
FULL OUTER JOIN `Current_Population_Data.GeoDivision`  c
on c.GEDIV=b.GEDIV
GROUP BY a.RACE, c.DIVISION

--This query gives the results of the total white household population in the United States and it is broken down into nine geographical divisions. 
SELECT count(b.HRHHID) as WhitePop, c.DIVISION
FROM `Current_Population_Data.EncryptionCode1`  a
FULL OUTER JOIN `Current_Population_Data.PopulationData`  b
on a.PTDTRACE=b.PTDTRACE
FULL OUTER JOIN `Current_Population_Data.GeoDivision`  c
on c.GEDIV=b.GEDIV
WHERE a.RACE="White"
GROUP BY c.DIVISION

--This query shows the total count of households in the New England division and the count is specified by race. 
SELECT a.RACE, count(b.HRHHID) as TotalHouseHold
FROM `Current_Population_Data.EncryptionCode1`  a
FULL OUTER JOIN `Current_Population_Data.PopulationData`  b
on a.PTDTRACE=b.PTDTRACE
FULL OUTER JOIN `Current_Population_Data.GeoDivision`  c
on c.GEDIV=b.GEDIV
WHERE c.DIVISION='New England'
GROUP BY a.RACE

--This query shows the total household count for each race in each geographical division in the United States broken down into gender.
SELECT a.RACE, c.DIVISION, d.GENDER, COUNT(b.HRHHID) as TotalHouseHold
FROM `Current_Population_Data.EncryptionCode1`  a
FULL OUTER JOIN `Current_Population_Data.PopulationData`  b
on a.PTDTRACE=b.PTDTRACE
FULL OUTER JOIN `Current_Population_Data.GeoDivision`  c
on c.GEDIV=b.GEDIV
FULL OUTER JOIN `Current_Population_Data.Gender` d
on d.PESEX=b.PESEX
WHERE d.GENDER IS NOT NULL and a.RACE IS NOT NULL
GROUP BY a.RACE, c.DIVISION, d.GENDER

--This query gives the total count of males and female population broken down into geographical divisions in the United States.
SELECT a.GENDER, c.DIVISION, COUNT(b.HRHHID) as TotalHouseHold
FROM `Current_Population_Data.Gender` a
FULL OUTER JOIN `Current_Population_Data.PopulationData`  b
on a.PESEX=b.PESEX
FULL OUTER JOIN `Current_Population_Data.GeoDivision` c
on b.GEDIV=c.GEDIV
WHERE a.GENDER IS NOT NULL and c.DIVISION IS NOT NULL 
GROUP BY a.GENDER, c.DIVISION


