--This query gives the total household count in this dataset for the United States.
SELECT SUM(tableA.TotalHouseHold)
FROM
(SELECT b.DIVISION, COUNT(DISTINCT a.HRHHID) as TotalHouseHold
FROM `Current_Population_Data.PopulationData`  a
FULL OUTER JOIN `Current_Population_Data.GeoDivision`  b
on a.GEDIV=b.GEDIV
GROUP BY b.DIVISION) tableA

--This query shows the highest minority population in each region with their population size
SELECT tableA.RACE, d.LMP, d.DIVISION
FROM
(SELECT c.RACE, COUNT(DISTINCT a.HRHHID) as TotalHouseHold
FROM `healthy-kayak-216403.Current_Population_Data.PopulationData`  a
FULL OUTER JOIN `healthy-kayak-216403.Current_Population_Data.GeoDivision`   b
on a.GEDIV=b.GEDIV
FULL OUTER JOIN `healthy-kayak-216403.Current_Population_Data.EncryptionCode1` c
on a.PTDTRACE=c.PTDTRACE
GROUP BY b.DIVISION, c.RACE) tableA
INNER JOIN `healthy-kayak-216403.Current_Population_Data.Largest_Minority_Population_Per_Division` d
on tableA.TotalHouseHold = d.LMP

--This query shows the lowest minority population in each region with their population size
SELECT e.DIVISION, tableA.RACE, e.SMP
FROM
(SELECT a.RACE, c.DIVISION, COUNT(b.HRHHID) as TotalHouseHold
FROM healthy-kayak-216403.Current_Population_Data.EncryptionCode1 a
FULL OUTER JOIN healthy-kayak-216403.Current_Population_Data.PopulationData b
on a.PTDTRACE=b.PTDTRACE
FULL OUTER JOIN healthy-kayak-216403.Current_Population_Data.GeoDivision c
on c.GEDIV=b.GEDIV
GROUP BY a.RACE, c.DIVISION) tableA
INNER JOIN healthy-kayak-216403.Current_Population_Data.Smallest_Minority_Population_Per_Division e
on e.SMP=tableA.TotalHouseHold
GROUP BY e.DIVISION, tableA.RACE, e.SMP

--This query gives the average regional population size for each race in the dataset for the United States
SELECT RACE, DIVISION, SUM(TotalHousehold) As AvgHouseCount
FROM (SELECT a.RACE, c.DIVISION, COUNT(b.HRHHID) as TotalHouseHold
FROM `healthy-kayak-216403.Current_Population_Data.EncryptionCode1`  a
FULL OUTER JOIN `healthy-kayak-216403.Current_Population_Data.PopulationData`  b
on a.PTDTRACE=b.PTDTRACE
FULL OUTER JOIN `healthy-kayak-216403.Current_Population_Data.GeoDivision`  c
on c.GEDIV=b.GEDIV
GROUP BY a.RACE, c.DIVISION)
Group BY Race, Division

--The query gives the average regional population size in the dataset for the United States
SELECT Division, AVG(tableA.TotalHouseHold)
FROM
(SELECT b.DIVISION, COUNT(DISTINCT a.HRHHID) as TotalHouseHold
FROM `Current_Population_Data.PopulationData`  a
FULL OUTER JOIN `Current_Population_Data.GeoDivision`  b
on a.GEDIV=b.GEDIV
GROUP BY b.DIVISION) tableA
GROUP BY DIVISION
