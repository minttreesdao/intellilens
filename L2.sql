
------------------ date
CREATE OR REPLACE TABLE intellilens.L2_date AS
SELECT date FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2022-01-01'), DATE('2025-12-31'), INTERVAL 1 DAY)) AS date;


------------------ profile
CREATE OR REPLACE TABLE intellilens.L2_profile AS
SELECT
  pr.profile_id
, pm.name AS profile_name
, pr.owned_by AS profile_address
, pr.is_burnt AS profile_burnt_flag
, CAST(MAX(pl.last_logged_in) AS DATE) AS profile_last_logged_in_date
, CAST(MAX(pl.last_logged_in) AS TIME) AS profile_last_logged_in_time
, CAST(pr.block_timestamp AS DATE) AS profile_creation_date
, total_posts AS profile_posts
, total_comments AS profile_comments
, total_mirrors AS profile_mirrors
, total_quotes AS profile_quotes
, total_publications AS profile_publications
, total_reacted AS profile_reacted
, total_reactions AS profile_reactions
, total_collects AS profile_collects
, total_acted AS profile_acted
, total_followers AS profile_followers
, total_following AS profile_following
, qp.score
FROM `lens-public-data.v2_polygon.profile_record` pr
LEFT JOIN (SELECT *
  FROM (SELECT pm.name, pm.profile_id, ROW_NUMBER() OVER(PARTITION BY pm.profile_id ORDER BY pm.block_timestamp DESC) AS rn
    FROM `lens-public-data.v2_polygon.profile_metadata` pm)
  WHERE rn = 1
) pm
  ON pr.profile_id = pm.profile_id
LEFT JOIN `lens-public-data.v2_polygon.profile_last_logged_in` pl
  ON pl.profile_id = pr.profile_id
LEFT JOIN `lens-public-data.v2_polygon.global_stats_profile` gsp
  ON gsp.profile_id = pr.profile_id
LEFT JOIN `lens-public-data.v2_polygon.global_stats_profile_follower` gspf
   ON gspf.profile_id = pr.profile_id
LEFT JOIN `lens-public-data.v2_polygon.machine_learning_quality_profiles` qp
   ON qp.profile_id = pr.profile_id
GROUP BY pr.profile_id, pm.name, pr.owned_by, pr.is_burnt, total_posts, total_comments, total_mirrors, total_quotes, total_publications, total_reacted
, total_reactions, total_collects, total_acted, total_followers, total_following, CAST(pr.block_timestamp AS DATE), qp.score
;


------------------ publication
CREATE OR REPLACE TABLE intellilens.L2_publication 
PARTITION BY publication_date_year_month
AS
SELECT 
  pr.publication_id
, pr.profile_id
, pr.publication_type
, pr.is_hidden AS publication_hidden_flag
, pr.is_momoka AS publication_momoka_flag
, pr.gardener_flagged AS publication_gardener_flagged_flag
, pr.app AS publication_app
, pm.language AS publication_language
, pm.content_warning AS publication_content_warning
, pm.main_content_focus AS publication_main_content_focus
, pm.tags_vector AS publication_tags_vector
, pm.is_encrypted AS publication_encryption_flag
, COALESCE(prr.currency, pc.currency)  AS publication_currency
, COALESCE(prr.amount, pc.amount) AS publication_revenue
, CAST(pr.block_timestamp AS date) AS publication_date
, CAST(pr.block_timestamp AS time) AS publication_time
, gsp.total_amount_of_collects AS publication_total_collects
, gsp.total_amount_of_mirrors  AS publication_total_mirrors
, gsp.total_amount_of_comments AS publication_total_comments
, gsp.total_amount_of_quotes AS publication_total_quotes
, gsp.total_reactions AS publication_total_reactios
, gsp.total_bookmarks AS publication_total_bookmarks
, gsp.total_amount_of_acted  AS publication_total_acted
, gsp.total_amount_of_comments_gardeners  AS publication_total_comments_gardeners
, CAST(DATETIME_TRUNC(pr.block_timestamp, MONTH) AS DATE) AS publication_date_year_month
FROM `lens-public-data.v2_polygon.publication_record` pr
LEFT JOIN `lens-public-data.v2_polygon.publication_metadata` pm
  ON pm.publication_id = pr.publication_id
LEFT JOIN intellilens.stage_v2_publication_collect prr
  ON prr.publication_id = pr.publication_id
LEFT JOIN `lens-public-data.v2_polygon.global_stats_publication` gsp
  ON gsp.publication_id = pr.publication_id
LEFT JOIN (SELECT post_id, currency, SUM(amount) AS amount
  FROM intellilens.stage_v1_publication_collect  pc
  GROUP BY post_id, currency) pc
  ON pc.post_id = pr.publication_id
;


------------------ interaction
CREATE OR REPLACE TABLE intellilens.L2_interaction 
PARTITION BY interaction_date_year_month
AS
SELECT *
, CAST(DATETIME_TRUNC(interaction_date, MONTH) AS DATE) AS interaction_date_year_month
FROM 
(
      
-- follow
SELECT
  CAST(NULL AS STRING) AS publication_id
, pf.profile_id
, CAST(pf.block_timestamp AS DATE) AS interaction_date
, CAST(pf.block_timestamp AS TIME) AS interaction_time
, pf.profile_follower_id AS interaction_profile_id
, CAST(NULL AS STRING) AS interaction_publication_id
, CAST(NULL AS STRING) AS interaction_app
, CAST(NULL AS STRING) AS interaction_language
, 'FOLLOW' AS interaction_type
, 0 AS amount_USD
FROM `lens-public-data.v2_polygon.profile_follower` pf

UNION ALL

-- comment, mirror, quote  
SELECT
  prp.publication_id
, prp.profile_id
, CAST(pr.block_timestamp AS DATE) AS interaction_date
, CAST(pr.block_timestamp AS TIME) AS interaction_time
, pr.profile_id AS interaction_profile_id
, pr.publication_id AS interaction_publication_id
, pr.app AS interaction_app
, pm.language AS interaction_language
, pr.publication_type AS interaction_type
, 0 AS amount_USD
FROM `lens-public-data.v2_polygon.publication_record` pr
LEFT JOIN `lens-public-data.v2_polygon.publication_metadata` pm
  ON pm.publication_id = pr.publication_id
LEFT JOIN `lens-public-data.v2_polygon.publication_record` prp
  ON prp.publication_id = pr.parent_publication_id
WHERE pr.parent_publication_id IS NOT NULL

UNION ALL

-- collect v2
SELECT   
    pom.publication_id
  , pr.profile_id
  , CAST(pom.block_timestamp AS DATE) AS interaction_date
  , CAST(pom.block_timestamp AS TIME) AS interaction_time
  , acted_profile_id AS interaction_profile_id
  , CAST(NULL AS STRING) AS interaction_publication_id
  , CAST(NULL AS STRING) AS interaction_app
  , CAST(NULL AS STRING) AS interaction_language
  , CASE WHEN is_collect THEN
      CASE WHEN COALESCE(CAST(poa.amount AS BIGNUMERIC), 0) > 0 THEN  'PAID COLLECT' ELSE 'FREE COLLECT' END
    END AS interaction_type
  , pc.amount_USD
FROM `lens-public-data.v2_polygon.publication_open_action_module_acted_record` pom
LEFT JOIN `lens-public-data.v2_polygon.publication_record` pr
  ON pom.publication_id = pr.publication_id
LEFT JOIN `lens-public-data.v2_polygon.publication_open_action_module` poa
  ON poa.publication_id = pom.publication_id
LEFT JOIN intellilens.stage_v2_publication_collect pc
  ON pc.publication_id = pom.publication_id

UNION ALL

-- collect v1
SELECT   
    pc.post_id AS publication_id
  , pc.profile_id
  , CAST(TIMESTAMP_MICROS(pc.collect_date) AS DATE) AS interaction_date
  , CAST(TIMESTAMP_MICROS(pc.collect_date) AS TIME) AS interaction_time
  , pc.profile_id_collected AS interaction_profile_id
  , CAST(NULL AS STRING) AS interaction_publication_id
  , CAST(NULL AS STRING) AS interaction_app
  , CAST(NULL AS STRING) AS interaction_language
  , CASE WHEN COALESCE(amount, 0) > 0 THEN 'PAID COLLECT' ELSE 'FREE COLLECT' END AS interaction_type
  , COALESCE(pc.amount, 0) * COAlESCE(ce.value, 1) AS amount_USD
FROM intellilens.stage_v1_publication_collect  pc
LEFT JOIN intellilens.stage_currency_exchange ce
  ON ce.currency = pc.currency
WHERE pc.collect_date IS NOT NULL

UNION ALL

-- upvote, downvote
SELECT   
    pre.publication_id
  , pr.profile_id
  , CAST(pre.action_at AS DATE) AS interaction_date
  , CAST(pre.action_at AS TIME) AS interaction_time
  , pre.actioned_by_profile_id AS interaction_profile_id
  , CAST(NULL AS STRING) AS interaction_publication_id
  , pre.app AS interaction_app
  , CAST(NULL AS STRING) AS interaction_language
  , pre.type AS interaction_type
  , 0 AS amount_USD
FROM `lens-public-data.v2_polygon.publication_reaction` pre
LEFT JOIN `lens-public-data.v2_polygon.publication_record` pr
  ON pr.publication_id = pre.publication_id

UNION ALL

-- mention
SELECT   
    CAST(NULL AS STRING) AS publication_id
  , pme.profile_id
  , CAST(pme.timestamp AS DATE) AS interaction_date
  , CAST(pme.timestamp AS TIME) AS interaction_time
  , pr.profile_id AS interaction_profile_id
  , pme.publication_id AS interaction_publication_id
  , pr.app AS interaction_app
  , pm.language AS interaction_language
  , 'MENTION' AS interaction_type
  , 0 AS amount_USD
FROM `lens-public-data.v2_polygon.publication_mention` pme
LEFT JOIN `lens-public-data.v2_polygon.publication_record` pr
  ON pr.publication_id = pme.publication_id
LEFT JOIN `lens-public-data.v2_polygon.publication_metadata` pm
  ON pm.publication_id = pr.publication_id
)
;


------------------ handle_history
SELECT 
  poh.profile_id 
, pr.profile_id AS profile_record_id
, poh.owned_by
, nhh.owned_by
, pr.owned_by
, nhh.handle
, poh.block_timestamp
, CASE WHEN ROW_NUMBER() OVER(PARTITION BY poh.profile_id ORDER BY poh.block_timestamp ASC) > 1 THEN TRUE ELSE FALSE END AS profile_changed_flag
, CASE WHEN ROW_NUMBER() OVER(PARTITION BY nhh.handle ORDER BY poh.block_timestamp ASC) > 1 THEN TRUE ELSE FALSE END AS address_changed_flag
FROM
( SELECT profile_id, owned_by, MAX(block_timestamp) AS block_timestamp
  FROM `lens-public-data.v2_polygon.profile_ownership_history`
  GROUP BY profile_id, owned_by) poh
LEFT JOIN `lens-public-data.v2_polygon.namespace_handle_history` nhh
  ON nhh.owned_by = poh.owned_by
LEFT JOIN `lens-public-data.v2_polygon.profile_record` pr
  ON nhh.owned_by = pr.owned_by
WHERE poh.profile_id in ('0x0f85', '0x010c69', '0x05', '0x139f')
ORDER BY poh.profile_id, nhh.handle, nhh.block_timestamp desc
;
