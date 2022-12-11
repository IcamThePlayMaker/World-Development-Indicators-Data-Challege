----  List of below countries would be targeted country for further analysis and would be visualize in dashboard---
--- This indicator would be crucial for futher analysis because with below query we could know the distribution of countries in the world which need help for food production and also which countries are already close enough to achive SDG-Zero Hunger---
---  as remainder the business model will focus on top 10 of countries that undernourishment ---- 


#overview filter
SELECT * 
FROM `bitlabs-dab.worldbank_wdi.data` 
WHERE indicator_name like '%undernourishment%' ; 


# specific filter level 1
SELECT 
  distinct(country_name),
  round(avg(value) over(partition by country_name),2) as average_Value_all_year,
  indicator_name
  
FROM 
  `bitlabs-dab.worldbank_wdi.data`
WHERE 
  indicator_name like '%undernourishment%'
  order by 2 desc  ;


#Final filter
SELECT 
  distinct(country_name),
  round(avg(value) over(partition by country_name),2) as average_Value_all_year,
  indicator_name
  
FROM 
  `bitlabs-dab.worldbank_wdi.data`
WHERE 
  indicator_name 
      like '%undernourishment%' and
   country_name 
      not in ("Korea, Dem. People's Rep.", "Central African Republic") #excluded union countries & closed country 

order by 2 desc 
limit 10;



--- based on above query we knew which segmented country would focus to further analysis---
--- So the next query below would be about pull data based on many criterias that can help for solution and would be save to csv file---

SELECT 
  row_number() over(partition by indicator_name order by year,value desc) as row_number,
  country_name,
  value,
  indicator_name,
  year
FROM 
  `bitlabs-dab.worldbank_wdi.data`
WHERE 
  indicator_name in
    (
      'Agricultural irrigated land (% of total agricultural land)',
      'Agricultural land (% of land area)',
      'Agricultural machinery, tractors per 100 sq. km of arable land',
      'Agriculture, forestry, and fishing, value added (% of GDP)',
      'Arable land (% of land area)',
      'Cereal yield (kg per hectare)',
      'Crop production index (2014-2016 = 100)',
      'Employment in agriculture, female (% of female employment) (modeled ILO estimate)',
      'Employment in agriculture, male (% of male employment) (modeled ILO estimate)',
      'Fertilizer consumption (kilograms per hectare of arable land)',
      'Food production index (2014-2016 = 100)',
      'Forest area (% of land area)',
      'Land under cereal production (hectares)',
      'Livestock production index (2014-2016 = 100)',
      'Permanent cropland (% of land area)',
      'Government expenditure on education, total (% of GDP)',
      'Literacy rate, youth total (% of people ages 15-24)',
      'Research and development expenditure (% of GDP)'
      ) and 
    country_name  in 
      ('Somalia',
      'Haiti',
      'Congo, Dem. Rep.',
      'Liberia',                            
      'Madagascar',
      'Sierra Leone' ,
      'Chad' ,
      'Rwanda' ,
      'Congo, Rep.' ,
      'Angola') ;


---- ANALYZE IMPORTANT METRICS

#goverment support for their education, R&D, and literacy rate condition in the country (distribution in average)
SELECT 
  distinct country_name,
  round(avg(value) over(partition by country_name,indicator_name),2) as average_Value_all_year,
  indicator_name
  
FROM 
  `bitlabs-dab.worldbank_wdi.data`
where 
  indicator_name in ( 'Government expenditure on education, total (% of GDP)','Research and development expenditure (% of GDP)','Literacy rate, youth total (% of people ages 15-24)') 
    and
  country_name  in 
      ('Somalia',
      'Haiti',
      'Congo, Dem. Rep.',
      'Liberia',                            
      'Madagascar',
      'Sierra Leone' ,
      'Chad' ,
      'Rwanda' ,
      'Congo, Rep.' ,
      'Angola')
order by 2 desc ;

# land condition indicator corresponding for agriculture activity (distribution in average)
WITH lc as (
SELECT 
  distinct country_name,
   indicator_name,
  round(avg(value) over(partition by indicator_name,country_name),2) as average_Value_all_year
 
FROM 
  `bitlabs-dab.worldbank_wdi.data`
WHERE 
  indicator_name in ( 'Agricultural irrigated land (% of total agricultural land)',
      'Agricultural land (% of land area)','Arable land (% of land area)','Forest area (% of land area)') 
    AND
  country_name  in 
      ('Somalia',
      'Haiti',
      'Congo, Dem. Rep.',
      'Liberia',                            
      'Madagascar',
      'Sierra Leone' ,
      'Chad' ,
      'Rwanda' ,
      'Congo, Rep.' ,
      'Angola')
order by 2 desc ,3 asc )

SELECT
  row_number() over(partition by country_name order by country_name desc,average_value_all_year desc)as row_number,
  country_name,
  indicator_name,
  average_value_all_year
FROM 
  lc ;


# employment in agriculture condition (distribution in average)
WITH EA as (
SELECT 
  distinct country_name,
  round(avg(value) over(partition by indicator_name,country_name),2) as average_Value_all_year,
  indicator_name

FROM 
  `bitlabs-dab.worldbank_wdi.data`
WHERE 
  indicator_name in ('Employment in agriculture, female (% of female employment) (modeled ILO estimate)',
      'Employment in agriculture, male (% of male employment) (modeled ILO estimate)' ) 
    AND
  country_name  in 
      ('Somalia',
      'Haiti',
      'Congo, Dem. Rep.',
      'Liberia',                            
      'Madagascar',
      'Sierra Leone' ,
      'Chad' ,
      'Rwanda' ,
      'Congo, Rep.' ,
      'Angola')
order by 2 desc ,3 asc )

SELECT 
row_number() over(partition by country_name order by average_value_all_year desc,country_name desc) as row_number,
  country_name,
  indicator_name,
  average_value_all_year
from EA ;

