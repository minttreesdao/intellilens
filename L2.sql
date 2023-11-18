
CREATE OR REPLACE TABLE intellilens.md_allowlist AS
SELECT '0x01bbee' AS profile_id UNION ALL -- mazemari
SELECT '0x0f85' AS profile_id UNION ALL -- alice
SELECT '0x05' AS profile_id UNION ALL -- stani
SELECT '0x73b1' AS profile_id UNION ALL -- jessy
SELECT '0x011e55' AS profile_id UNION ALL -- dankshard
SELECT '0x8807' AS profile_id UNION ALL -- siddxa
SELECT '0x0184ed' AS profile_id UNION ALL -- xexexe
SELECT '0x218b' AS profile_id UNION ALL -- carstenpoetter
SELECT '0xbee1' AS profile_id UNION ALL -- Vinod
SELECT '0x69f9' AS profile_id UNION ALL -- mycaleum
SELECT '0x43c3' AS profile_id UNION ALL -- hoylexgbrillaze
SELECT '0x0155a4' AS profile_id -- whale_code
;


----------------------------------------------------------------------------------------
------------------------------------------ STAGING -------------------------------------
----------------------------------------------------------------------------------------

-- stage_v1_publication_collect via gs://intellilens/stage_v1_publication_collect because of region difference v1 vs v2. 

CREATE OR REPLACE TABLE lens_public.publication_collect AS
SELECT pp.profile_id, pp.post_id, pp.block_timestamp AS publication_date, cr.block_timestamp AS collect_date, p.profile_id AS profile_id_collected, p.handle AS collected_by,
CASE  WHEN cmd.currency = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174' THEN 'USDC' 
      WHEN cmd.currency = '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270' THEN 'WMATIC' 
      WHEN cmd.currency = '0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619' THEN 'WETH' 
      WHEN cmd.currency = '0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063' THEN 'DAI' 
      WHEN cmd.currency = '0xD838290e877E0188a4A44700463419ED96c16107' THEN 'NCT' 
      ELSE cmd.currency
END AS currency,
CASE WHEN cmd.currency = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174' THEN ROUND(CAST(cmd.amount AS BIGNUMERIC)  / 1000000, 2)
      WHEN cmd.currency = '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270' THEN ROUND(CAST(cmd.amount AS BIGNUMERIC) / 1000000000000000000, 2)
      WHEN cmd.currency = '0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619' THEN ROUND(CAST(cmd.amount AS BIGNUMERIC) / 1000000000000000000, 2)
      WHEN cmd.currency = '0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063' THEN ROUND(CAST(cmd.amount AS BIGNUMERIC) / 1000000000000000000, 2)
      WHEN cmd.currency = '0xD838290e877E0188a4A44700463419ED96c16107' THEN ROUND(CAST(cmd.amount AS BIGNUMERIC) / 1000000000000000000, 2)
      ELSE CAST(cmd.amount AS BIGNUMERIC)
END AS amount
, cmd.referral_fee AS referral_fee_pct
, cr.referral_id 
FROM `lens-public-data.polygon.public_profile_post` pp
LEFT JOIN `lens-public-data.polygon.public_publication_collect_module_details` cmd 
  ON cmd.publication_id = pp.post_id
LEFT JOIN `lens-public-data.polygon.public_publication_collect_module_collected_records` cr
  ON cr.publication_id = pp.post_id
LEFT JOIN (SELECT handle, owned_by, profile_id
  FROM(
    SELECT p.handle, p.owned_by, p.profile_id,
    ROW_NUMBER() OVER(PARTITION by owned_by ORDER BY ps.total_collects DESC) AS rnk
    FROM `lens-public-data.polygon.public_profile` p
    LEFT JOIN `lens-public-data.polygon.public_profile_stats` ps
      ON ps.profile_id = p.profile_id  
  )
  WHERE rnk = 1
  ) p
  ON p.owned_by = cr.collected_by
ORDER BY pp.block_timestamp DESC
;

----------------------------------------------------------------------------------------
------------------------------------------ L2 ------------------------------------------
----------------------------------------------------------------------------------------

------------------ profile

SELECT
  pr.profile_id
, pm.name AS profile_name
, pr.owned_by AS profile_address
, pr.is_burnt AS profile_burnt_flag
, CAST(pl.last_logged_in AS DATE) AS profile_last_logged_in_date
, CAST(pl.last_logged_in AS TIME) AS profile_last_logged_in_time
, CAST(pr.block_timestamp AS DATE) AS profile_creation_date
FROM `lens-public-data.v2_polygon.profile_record` pr
LEFT JOIN `lens-public-data.v2_polygon.profile_metadata` pm
  ON pr.profile_id = pm.profile_id
LEFT JOIN `lens-public-data.v2_polygon.profile_last_logged_in` pl
  ON pl.profile_id = pr.profile_id
where pr.profile_id in ('0x0f85', '0x010c69', '0x05')
;


------------------ publication
CREATE OR REPLACE TABLE intellilens.L2_publication AS
SELECT 
  pr.publication_id
, pr.profile_id
, pr.publication_type
, pr.is_hidden AS publication_hidden_flag
, pr.is_momoka AS publication_momoka_flag
, pr.gardener_flagged AS publication_gardener_flagged_flag
, pr.app AS publication_app
, pm.language AS publication_language
-- , pm.region AS publication_region
, pm.content_warning AS publication_content_warning
, pm.main_content_focus AS publication_main_content_focus
, pm.tags_vector AS publication_tags_vector
, pm.is_encrypted AS publication_encryption_flag
, COALESCE(prr.currency, pc.currency)  AS publication_currency
, COALESCE(prr.amount, pc.amount) AS publication_revenue
, prr.fiat_price_snapshot AS fiat_price
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
FROM `lens-public-data.v2_polygon.publication_record` pr
LEFT JOIN `lens-public-data.v2_polygon.publication_metadata` pm
  ON pm.publication_id = pr.publication_id
LEFT JOIN (
  SELECT 
    publication_id 
  , CASE  WHEN prr.currency = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174' THEN 'USDC' 
          WHEN prr.currency = '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270' THEN 'WMATIC' 
          WHEN prr.currency = '0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619' THEN 'WETH' 
          WHEN prr.currency = '0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063' THEN 'DAI' 
          WHEN prr.currency = '0xD838290e877E0188a4A44700463419ED96c16107' THEN 'NCT' 
          ELSE prr.currency END AS currency
  , SUM(CASE  WHEN prr.currency = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174' THEN ROUND(CAST(prr.amount AS BIGNUMERIC) / 1000000, 2)
              WHEN prr.currency = '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270' THEN ROUND(CAST(prr.amount AS BIGNUMERIC) / 1000000000000000000, 2)
              WHEN prr.currency = '0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619' THEN ROUND(CAST(prr.amount AS BIGNUMERIC) / 1000000000000000000, 2)
              WHEN prr.currency = '0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063' THEN ROUND(CAST(prr.amount AS BIGNUMERIC) / 1000000000000000000, 2)
              WHEN prr.currency = '0xD838290e877E0188a4A44700463419ED96c16107' THEN ROUND(CAST(prr.amount AS BIGNUMERIC) / 1000000000000000000, 2)
              ELSE CAST(prr.amount AS BIGNUMERIC) END) AS amount
  , fiat_price_snapshot
  FROM `lens-public-data.v2_polygon.publication_revenue_record` prr
  GROUP BY publication_id, currency, fiat_price_snapshot
  ) prr
  ON prr.publication_id = pr.publication_id
LEFT JOIN `lens-public-data.v2_polygon.global_stats_publication` gsp
  ON gsp.publication_id = pr.publication_id
LEFT JOIN (SELECT post_id, currency, SUM(amount) AS amount
  FROM intellilens.stage_v1_publication_collect  pc
  GROUP BY post_id, currency) pc
  ON pc.post_id = pr.publication_id
ORDER BY pr.publication_id DESC, pr.block_timestamp DESC
;


select publication_id, publication_revenue, publication_currency
from intellilens.L2_publication 
where publication_id in ('0x0f85-0x1d26', '0x019b72-0x0181', '0xb43c-0x0235')
------------------ interaction












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
-- WHERE handle in ('lens/nftfandome', 'lens/rinfinity')
ORDER BY poh.profile_id, nhh.handle, nhh.block_timestamp desc

