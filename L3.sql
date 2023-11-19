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

CREATE OR REPLACE TABLE intellilens.L3_publication_year_month AS
SELECT
  p.profile_id
, SUBSTRING(CAST(p.publication_date AS STRING), 1, 7) AS year_month
, p.publication_type
, count(*) AS number_publication
FROM intellilens.L3_publication p
GROUP BY p.profile_id, year_month, p.publication_type
;

CREATE OR REPLACE TABLE intellilens.L3_user_analysis AS
SELECT 
  i.interaction_profile_id AS user_id
, p.profile_name
, i.profile_id
, MIN(interaction_date) AS first_interaction_date
, MAX(interaction_date) AS last_interaction_date
, COUNT(DISTINCT CONCAT(p.profile_id, '_', interaction_type)) AS number_interaction
, CASE WHEN interaction_type IN ('COMMENT', 'MIRROR', 'PAID COLLECT', 'FREE COLLECT', 'QUOTE', 'FOLLOW') THEN 'high_value'
  ELSE 'low_value' END AS interaction_category
, SUM(CASE WHEN interaction_type = 'FOLLOW' THEN 111
       WHEN interaction_type = 'PAID COLLECT' THEN 100
       WHEN interaction_type = 'COMMENT' THEN 60 
       WHEN interaction_type = 'QUOTE' THEN 45
       WHEN interaction_type = 'FREE COLLECT' THEN 40 
       WHEN interaction_type = 'MIRROR' THEN 35 
       WHEN interaction_type = 'MENTION' THEN 15 
       WHEN interaction_type = 'UPVOTE' THEN 15 
       ELSE 0 END) AS user_score
, SUM(amount_USD) AS amount_USD
FROM `intellilens.L3_interaction` i
LEFT JOIN `intellilens.L3_profile` p
  ON p.profile_id = i.interaction_profile_id
WHERE p.profile_name IS NOT NULL
GROUP BY i.interaction_profile_id, profile_id, interaction_category, profile_name
;
