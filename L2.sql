
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
SELECT 
  pr.publication_id
, pr.profile_id
, pr.publication_type
, pr.is_hidden AS publication_hidden_flag
, pr.is_momoka AS publication_momoka_flag
, pr.gardener_flagged AS publication_gardener_flagged_flag
, pr.app AS publication_app
, CAST(pr.block_timestamp AS date) AS publication_date
, CAST(pr.block_timestamp AS time) AS publication_time
FROM `lens-public-data.v2_polygon.publication_record` pr
where pr.profile_id in ('0x0f85')
ORDER BY pr.block_timestamp DESC
;

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
