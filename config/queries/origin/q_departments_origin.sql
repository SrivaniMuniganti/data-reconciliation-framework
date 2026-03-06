-- q_departments_origin.sql
-- Origin query: Departments per Group

SELECT DISTINCT
    g.group_id              AS origin_group_id,
    CASE
        WHEN da.name IN ('Default', 'All Locations') THEN 'Campus-Wide'
        ELSE da.name
    END                     AS dept_name,
    COUNT(DISTINCT da.name) AS record_count,
    'unit'::TEXT            AS dept_type,
    'origin_group_id,dept_name'::TEXT AS unique_key
FROM dept_aggregate da
JOIN org_unit g ON g.id = da.org_unit_id
WHERE g.group_id IN (
    -- TODO: replace with your origin system group IDs
    :org_group_ids
)
GROUP BY
    g.group_id,
    da.name
ORDER BY
    g.group_id;
