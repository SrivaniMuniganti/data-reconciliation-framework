-- q_feature_flags_origin.sql
-- Origin query: Feature Flags per Org Unit

SELECT
    org_unit_id         AS origin_site_id,
    feature_key         AS feature_flag
FROM feature_flag_enabled
WHERE org_unit_id IN (
    -- TODO: replace with your origin system org unit IDs
    :org_unit_ids
);
