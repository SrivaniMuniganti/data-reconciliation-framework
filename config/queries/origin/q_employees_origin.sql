-- q_employees_origin.sql
-- Origin query: Employee records

SELECT
    u.org_unit_id,
    u.email_address             AS email,
    u.first_name || ' ' || u.last_name AS full_name,
    u.id                        AS employee_id,
    COUNT(DISTINCT u.id)        AS record_count,
    'employee'::TEXT            AS entity_type
FROM app_user u
JOIN org_unit o
    ON o.id = u.org_unit_id
JOIN user_role r
    ON r.id = u.role_id
JOIN pay_code pc
    ON pc.id = u.pay_code_id
JOIN employment_status es
    ON es.id = u.employment_status_id
    AND es.description IN ('active', 'terminated', 'inactive')
JOIN employment_type et
    ON et.id = u.employment_type_id
    AND et.code NOT IN ('agency', 'contractor')
WHERE (
    es.description = 'active'
    OR (es.description = 'terminated' AND u.end_date > :termination_cutoff_date)
    OR es.description = 'inactive'
)
  AND u.home_org_unit_id IS NULL
  AND u.org_unit_id IN (
      -- TODO: replace with your origin system org unit IDs
      :org_unit_ids
  )
  AND u.id NOT IN (
      -- TODO: add any user IDs to explicitly exclude
      :excluded_user_ids
  )
  AND u.created_date <= :created_before_date
GROUP BY
    u.id,
    u.org_unit_id,
    u.email_address,
    u.first_name || ' ' || u.last_name
ORDER BY
    u.id ASC,
    u.org_unit_id ASC;
