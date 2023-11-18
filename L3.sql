CREATE OR REPLACE TABLE intellilens.L3_publication AS
SELECT p.*
FROM intellilens.L2_publication p 
JOIN intellilens.md_allowlist a
ON p.profile_id = a.profile_id
;


CREATE OR REPLACE TABLE intellilens.L3_interaction AS
SELECT i.*
FROM intellilens.L2_interaction i 
JOIN intellilens.md_allowlist a
ON i.profile_id = a.profile_id
;


CREATE OR REPLACE TABLE intellilens.L3_interaction_year_month AS
SELECT
  i.profile_id
, SUBSTRING(CAST(i.interaction_date AS STRING), 1, 7) AS year_month
, i.interaction_type
, COUNT(distinct i.interaction_profile_id) AS number_user
, COUNT(*) AS number_interaction
, SUM(i.amount_USD) AS revenue_USD
FROM intellilens.L3_interaction i
GROUP BY i.profile_id, year_month, i.interaction_type
;


CREATE OR REPLACE TABLE intellilens.L3_publication_year_month AS
SELECT
  p.profile_id
, SUBSTRING(CAST(p.publication_date AS STRING), 1, 7) AS year_month
, p.publication_type
, count(*) AS number_publication
FROM intellilens.L3_publication p
GROUP BY p.profile_id, year_month, p.publication_type
;
