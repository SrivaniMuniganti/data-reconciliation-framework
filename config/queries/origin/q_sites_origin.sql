-- q_sites_origin.sql
-- Origin query: Org Units / Sites

SELECT
    group_id            AS origin_group_id,
    id                  AS origin_site_id,
    name,
    short_code,
    time_zone,
    external_ref_id
FROM org_unit
WHERE NOT is_disabled
  AND NOT is_demo
  AND NOT is_archived
  AND id IN (
      -- TODO: replace with your origin system org unit IDs
      :org_unit_ids
  );
