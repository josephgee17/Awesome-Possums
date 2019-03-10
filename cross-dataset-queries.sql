--This query will look at proportionality by comparing the percentage of race population with the percentage of awards received group by race.
SELECT a.race, b.PopData_PopulationSize_Percentage, a.Percent_of_Awards
FROM
(SELECT race, count(award), round(count(award)/441,3) as Percent_of_Awards
FROM `healthy-kayak-216403.PopulationData.Formatted_OscarDemo`
WHERE race = "White" or race = "Black" or race = "Asian" or race = "Multiracial"
GROUP BY race) a
INNER JOIN 
(SELECT b.race, round(count(DISTINCT a.HRHHID)/54681,3) as PopData_PopulationSize_Percentage
FROM `healthy-kayak-216403.PopulationData.Formatted_PopDataAll` as a
INNER JOIN `healthy-kayak-216403.PopulationData.Race` as b
on a.PTDTRACE = b.PTDTRACE
WHERE b.race = "White" or b.race = "Black" or b.race = "Asian" or b.race = "Multiracial"
GROUP BY b.race) b
on a.race = b.race
GROUP BY a.race, a.Percent_of_Awards, b.PopData_PopulationSize_Percentage
ORDER BY b.PopData_PopulationSize_Percentage DESC

--This query will compare the percentage of race population with the percentage of the actor/actress's race population by award type.
SELECT a.race, b.PopData_PopulationSize_Percentage, a.award, a.Percent_of_Awards_By_AwardType
FROM
(SELECT race, award, count(award), round(count(award)/441,3) as Percent_of_Awards_By_AwardType
FROM `healthy-kayak-216403.PopulationData.Formatted_OscarDemo`
WHERE race = "White" or race = "Black" or race = "Asian" or race = "Multiracial"
GROUP BY race, award) a
INNER JOIN 
(SELECT b.race, round(count(DISTINCT a.HRHHID)/54681,3) as PopData_PopulationSize_Percentage
FROM `healthy-kayak-216403.PopulationData.Formatted_PopDataAll` as a
INNER JOIN `healthy-kayak-216403.PopulationData.Race` as b
on a.PTDTRACE = b.PTDTRACE
WHERE b.race = "White" or b.race = "Black" or b.race = "Asian" or b.race = "Multiracial"
GROUP BY b.race) b
on a.race = b.race
GROUP BY a.race, a.Percent_of_Awards_By_AwardType, b.PopData_PopulationSize_Percentage, a.award
ORDER BY b.PopData_PopulationSize_Percentage DESC

--This query will compare the percentage of race population with the percentage of actor/actress's awards by race from year 2000 and up (2000-2010).
SELECT a.race, b.PopData_PopulationSize_Percentage, sum(a.Percent_of_Awards) as Percent_of_Awards
FROM
(SELECT award_year, race, count(award), round(count(award)/82,3) as Percent_of_Awards
FROM `healthy-kayak-216403.PopulationData.Formatted_OscarDemo`
WHERE race = "White" or race = "Black" or race = "Asian" or race = "Multiracial"
GROUP BY race, award_year) a
INNER JOIN 
(SELECT b.race, round(count(DISTINCT a.HRHHID)/54681,3) as PopData_PopulationSize_Percentage
FROM `healthy-kayak-216403.PopulationData.Formatted_PopDataAll` as a
INNER JOIN `healthy-kayak-216403.PopulationData.Race` as b
on a.PTDTRACE = b.PTDTRACE
WHERE b.race = "White" or b.race = "Black" or b.race = "Asian" or b.race = "Multiracial"
GROUP BY b.race) b
on a.race = b.race
WHERE a.award_year >= 2000
GROUP BY a.race, b.PopData_PopulationSize_Percentage
ORDER BY b.PopData_PopulationSize_Percentage DESC

--This query will compare the percentage of race population in the US with the percentage of race population in each award type from 2000 and up (2000-2010).
SELECT a.race, b.PopData_PopulationSize_Percentage, a.award, round(sum(a.Percent_of_Awards),3) as Percent_of_AwardType
FROM
(SELECT award, award_year, race, count(award), round(count(award)/82,3) as Percent_of_Awards
FROM `healthy-kayak-216403.PopulationData.Formatted_OscarDemo`
WHERE race = "White" or race = "Black" or race = "Asian" or race = "Multiracial"
GROUP BY race, award_year, award) a
INNER JOIN 
(SELECT b.race, round(count(DISTINCT a.HRHHID)/54681,3) as PopData_PopulationSize_Percentage
FROM `healthy-kayak-216403.PopulationData.Formatted_PopDataAll` as a
INNER JOIN `healthy-kayak-216403.PopulationData.Race` as b
on a.PTDTRACE = b.PTDTRACE
WHERE b.race = "White" or b.race = "Black" or b.race = "Asian" or b.race = "Multiracial"
GROUP BY b.race) b
on a.race = b.race
WHERE a.award_year >= 2000
GROUP BY a.race, b.PopData_PopulationSize_Percentage, a.award
ORDER BY b.PopData_PopulationSize_Percentage DESC


