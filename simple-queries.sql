--This query for the USA_Census_2015.CensusData table and it gives the total population count for each state in the U.S., and it also has the population count broken down into men, women, and each racial demographic (Hispanic, White, Black, Native, Asian, and Pacific). The results are listed in alphabetical order by state name.

SELECT State, 
SUM(TotalPop) as TotalPop, 
SUM(Men) as Men, 
SUM(Women) as Women, 
ROUND(SUM((Hispanic/100)*TotalPop)) as HispanicPop, 
ROUND(SUM((White/100)*TotalPop)) as WhitePop, 
ROUND(SUM((Black/100)*TotalPop)) as BlackPop, 
ROUND(SUM((Native/100)*TotalPop)) as NativePop, 
ROUND(SUM((Asian/100)*TotalPop)) as AsianPop, 
ROUND(SUM((Pacific/100)*TotalPop)) as PacificPop
FROM [healthy-kayak-216403.USA_Census_2015.CensusData]
WHERE TotalPop IS NOT NULL
GROUP BY State
ORDER BY State

--This query is for the USA_Census_2015.CensusDataByCounty table and it gives the total population count for counties in the U.S., and it also has the population count broken down into men, women, and each racial demographic (Hispanic, White, Black, Native, Asian, and Pacific). The results are listed in alphabetical order by county name.

SELECT County, 
SUM(TotalPop) as TotalPop, 
SUM(Men) as Men, 
SUM(Women) as Women, 
ROUND(SUM((Hispanic/100)*TotalPop)) as HispanicPop, 
ROUND(SUM((White/100)*TotalPop)) as WhitePop, 
ROUND(SUM((Black/100)*TotalPop)) as BlackPop, 
ROUND(SUM((Native/100)*TotalPop)) as NativePop, 
ROUND(SUM((Asian/100)*TotalPop)) as AsianPop, 
ROUND(SUM((Pacific/100)*TotalPop)) as PacificPop
FROM [healthy-kayak-216403.USA_Census_2015.CensusDataByCounty]
WHERE TotalPop IS NOT NULL
GROUP BY County
ORDER BY County

--This query is for the Oscar_Academy_Awards_Demographics.Demographics table and it identifies actors/actresses in the year 2010 and gives their ethnicity, the year they received the award, and the award they received. The results are listed in alphabetical order by first name. 

SELECT person as Name,
race_ethnicity as Ethnicity,
year_of_award as Year_Awarded,
award as Award
FROM [healthy-kayak-216403.Oscar_Academy_Awards_Demographics.Demographics]
WHERE year_of_award = 2010
ORDER BY person

--This query is for the Oscar_Academy_Awards_Demographics.Demographics table and it shows the total count of awards for each ethnicity. The results are listed in descending order: from ethnicity with highest count to ethnicity with lowest count of awards.

SELECT race_ethnicity,
COUNT(award) as Total_Award_Count
FROM [healthy-kayak-216403.Oscar_Academy_Awards_Demographics.Demographics]
WHERE race_ethnicity IS NOT NULL
GROUP BY race_ethnicity
ORDER BY Total_Award_Count DESC

--This query is for the Oscar_Academy_Awards_Demographics.Demographics table and it shows the total count of awards given between 1950 and 1974 for each ethnicity. The results are listed in descending order: from ethnicity with highest count to ethnicity with lowest count of awards.

SELECT race_ethnicity,
COUNT(award) as Award_Count
FROM [healthy-kayak-216403.Oscar_Academy_Awards_Demographics.Demographics]
WHERE year_of_award between 1950 and 1974
GROUP BY race_ethnicity
ORDER BY Award_Count DESC

--This query is for the Oscar_Academy_Awards_Demographics.Demographics table and it shows the total count of awards given between 1985 and 2010 for each ethnicity. The results are listed in descending order: from ethnicity with highest count to ethnicity with lowest count of awards.

SELECT race_ethnicity,
COUNT(award) as Award_Count
FROM [healthy-kayak-216403.Oscar_Academy_Awards_Demographics.Demographics]
WHERE year_of_award between 1985 and 2009
GROUP BY race_ethnicity
ORDER BY Award_Count DESC

--This query is for the Current_Population_Data.EncryptionCode table and it identifies the codes used as row names for the demographic variables we want to look at: Ethnicity, Region, and Gender. 

SELECT string_field_0 as Code,
string_field_1 as Variable
FROM [Current_Population_Data.EncryptionCode] 
WHERE string_field_1 = 'RACE' OR string_field_1 = 'REGION' OR string_field_1 = 'SEX'
GROUP BY Code, Variable

--This query is for the Current_Population_Data.PopulationData table and it gives the total population of each ethnicity and it is broken down into 4 regions: Region 1 = NORTHEAST, Region 2 = MIDWEST (FORMERLY NORTH CENTRAL), Region 3 = SOUTH, and Region 4 = WEST.

SELECT COUNT(PTDTRACE = 01) as WhitePop,
COUNT(PTDTRACE = 02) as BlackPop,
COUNT(PTDTRACE = 03) as AmericanIndian_or_AlaskanNativePop,
COUNT(PTDTRACE = 04) as AsianPop,
COUNT(PTDTRACE = 05) as Hawaiian_or_PacificIslanderPop,
COUNT(PRDTHSP > 0) as HispanicPop
FROM [Current_Population_Data.PopulationData] 
WHERE GEREG IS NOT NULL
GROUP BY GEREG


