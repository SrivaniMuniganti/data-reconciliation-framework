-- q_flsa_code_map_origin.sql
-- Origin query: FLSA Code Mapping

SELECT
    im_org.group_id             AS origin_group_id,
    im_org.org_unit_id          AS origin_site_id,
    im.vendor_code              AS flsa_code,
    (im.internal_code::INT - 1) AS flsa_value
FROM integration_mapping im
JOIN integration_mapping_org im_org
    ON im_org.id = im.integration_mapping_org_id
WHERE im.mapping_type_id = 19
  AND im_org.org_unit_id IN (
      -- TODO: replace with your origin system org unit IDs
      :org_unit_ids
  );
