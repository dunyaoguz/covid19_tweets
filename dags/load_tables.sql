INSERT INTO public.covid19_stats (country, day, cases, deaths)
SELECT DISTINCT countriesAndTerritories, dateRep, cases, deaths
FROM staging_cases;

INSERT INTO public.country_stats (country, population)
SELECT DISTINCT countriesAndTerritories, popData2018
FROM staging_cases
WHERE popData2018 IS NOT NULL; 
