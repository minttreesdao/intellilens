-- md_allowlist
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


-- stage_currency_exchange
CREATE OR REPLACE TABLE intellilens.stage_currency_exchange AS
SELECT '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270' AS currency_address, 'WMATIC' AS currency, 0.8 AS value, 1000000000000000000 AS factor UNION ALL
SELECT '0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619' AS currency_address, 'WETH' AS currency, 1939 AS value, 1000000000000000000 AS factor UNION ALL
SELECT '0xD838290e877E0188a4A44700463419ED96c16107' AS currency_address, 'NCT' AS currency, 1.14 AS value, 1000000000000000000 AS factor UNION ALL
SELECT '0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063' AS currency_address, 'DAI' AS currency, 1 AS value, 1000000000000000000 AS factor UNION ALL
SELECT '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174' AS currency_address, 'USDC' AS currency, 1 AS value, 1000000 AS factor
;


-- stage_v2_publication_collect
CREATE OR REPLACE TABLE intellilens.stage_v2_publication_collect AS
SELECT
  publication_id
, prr.currency
, amount
, amount * COALESCE(ce.value, 1) AS amount_USD 
FROM(
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
  FROM `lens-public-data.v2_polygon.publication_open_action_module` prr
  GROUP BY publication_id, currency
) prr
LEFT JOIN intellilens.stage_currency_exchange ce
  ON ce.currency = prr.currency
;
