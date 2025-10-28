-- Task 1
-- creacion de las semanas
WITH contacts_weekly AS (
  SELECT
    contact_id,
    FORMAT_DATE('%Y-%W', DATE(created_at)) AS year_week
  FROM `bigdataarchitecture-453018.453018.contacts`
)
,
-- Uso de customer como conversión = Booleano
conversion_status AS (
  SELECT
    hs_object_id AS contact_id,
    MAX(CASE WHEN lifecyclestage = 'customer' THEN 1 ELSE 0 END) AS is_customer
  FROM `bigdataarchitecture-453018.453018.events`
  GROUP BY contact_id
)

SELECT
  contacts_weekly.year_week,
  COUNT(contacts_weekly.contact_id) AS total_contacts, 
  SUM(conversion_status.is_customer) AS total_customers,
 SAFE_DIVIDE(SUM(conversion_status.is_customer), COUNT(contacts_weekly.contact_id)) * 100 AS conversion_rate
FROM contacts_weekly 
LEFT JOIN conversion_status 
  ON contacts_weekly.contact_id = conversion_status.contact_id
GROUP BY year_week
ORDER BY conversion_rate DESC
;

-------------------------------------------------------------------------------
-- Task 2 Analisis de cohortes

-- preparar cohortes por semana
With contacts_cohorts AS (
  SELECT
   contact_id,
   DATE(created_at) as created_date,
   FORMAT_DATE("%Y-%W", DATE(created_at)) AS cohort_week
  FROM  `bigdataarchitecture-453018.453018.contacts`

)
,
-- desde primera vez que aparecen como customers
conversion AS(
  SELECT
   hs_object_id AS contact_id,
   MIN(DATE(lastmodified_ts)) AS conversion_date
  FROM `bigdataarchitecture-453018.453018.events`
  WHERE lifecyclestage = 'customer'
  GROUP BY contact_id
)
,
-- diferencias entre dia de creacion y conversion
cohort_analysis AS (
  SELECT
   contacts_cohorts.contact_id,
   contacts_cohorts.created_date,
   contacts_cohorts.cohort_week,
   conversion.conversion_date,
   DATE_DIFF(conversion.conversion_date, contacts_cohorts.created_date, DAY) AS days_to_convert
  FROM  contacts_cohorts
  LEFT JOIN conversion
  ON contacts_cohorts.contact_id = conversion.contact_id
)

SELECT
 cohort_week,
 COUNT(contact_id) AS total_contacts,
 -- primeros 7 dias logica booleana
 COUNT(CASE WHEN days_to_convert BETWEEN 0 AND 7 THEN contact_id END) AS converted_7d,
 --- 14 dias
 COUNT(CASE WHEN days_to_convert BETWEEN 0 AND 14 THEN contact_id END) AS converted_14d,
 --- % de conversion
 ROUND(SAFE_DIVIDE(COUNT(CASE WHEN days_to_convert BETWEEN 0 AND 7 THEN contact_id END),
         COUNT(contact_id)) * 100,2) AS conversion_rate_7d,
 ROUND(SAFE_DIVIDE(COUNT(CASE WHEN days_to_convert BETWEEN 0 AND 14 THEN contact_id END),
         COUNT(contact_id)) * 100,2) AS conversion_rate_14d
 FROM cohort_analysis
 GROUP BY cohort_week 
 ORDER BY cohort_week
 ;

 -----------------------------------------------------------------
-- Task 3 Matriz de transicion from_stage -> to_stage

WITH ordered_events AS (
  SELECT
   hs_object_id,
   lifecyclestage AS from_stage,
   lastmodified_ts,
   LEAD(lifecyclestage) OVER (PARTITION BY hs_object_id ORDER BY lastmodified_ts) AS to_stage
FROM `bigdataarchitecture-453018.453018.events`
)
SELECT *
FROM (
  SELECT
   from_stage,
   to_stage,
   COUNT(hs_object_id) AS num_contacts
  FROM ordered_events
  WHERE to_stage IS NOT NULL
  GROUP BY from_stage, to_stage
)
PIVOT(
  SUM(num_contacts) FOR to_stage IN('lead','marketingqualifiedlead','subscriber','customer'))
  ORDER BY from_stage
;

-------------------------------------------------------
-- Task 3b Matriz de trancision de percentil 80
-- 80% de los contactos tardó menos o igual en pasar a otra etapa
WITH ordered_events AS (
  SELECT
   hs_object_id,
   lifecyclestage AS from_stage,
   lastmodified_ts,
   LEAD(lifecyclestage) OVER (PARTITION BY hs_object_id ORDER BY lastmodified_ts) AS to_stage,
   LEAD(lastmodified_ts) OVER(PARTITION BY hs_object_id ORDER BY lastmodified_ts) AS next_ts
FROM `bigdataarchitecture-453018.453018.events`
)
,
transition_time AS (
  SELECT
   from_stage,
   to_stage,
   DATE_DIFF(DATE(next_ts), DATE(lastmodified_ts), DAY) AS days_to_convert
  FROM ordered_events
  WHERE to_stage IS NOT NULL 
)

SELECT *
FROM(
  SELECT
   from_stage,
   to_stage,
   APPROX_QUANTILES(days_to_convert,5)[OFFSET(4)] AS p80_days
  FROM transition_time
  GROUP BY from_stage,to_stage)
PIVOT(MAX(p80_days) FOR to_stage IN ('lead','marketingqualifiedlead','subscriber','customer'))
ORDER BY from_stage
;



