-- q_time_off_reasons_origin.sql
-- Origin query: Time-Off Reason Types

SELECT DISTINCT
    LTRIM(RTRIM(tr.name))   AS reason_value,
    g.group_id              AS origin_group_id,
    TRUE                    AS is_active
FROM time_off_reason tr
JOIN org_unit g
    ON g.id = tr.org_unit_id
    AND g.group_id IN (
        -- TODO: replace with your origin system group IDs
        :org_group_ids
    )
    AND g.id IN (
        -- TODO: replace with your origin system org unit IDs
        :org_unit_ids
    )
WHERE LOWER(tr.name) NOT LIKE '%do not use%'
ORDER BY 1;
