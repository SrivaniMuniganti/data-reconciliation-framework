-- q_departments_destination.sql
-- Destination query: Departments per Group

SELECT
    map.GroupId         AS dest_org_id,
    dept.Name           AS dept_name,
    'unit'              AS dept_type,
    COUNT(DISTINCT dept.Name) AS record_count
FROM dbo.[OrgGroup] dept
JOIN dbo.[OrgGroupType] gt
    ON dept.OrgGroupTypeId = gt.Id AND gt.Name = 'Department'
INNER JOIN dbo.OrgGroupMap map
    ON dept.Id = map.RelatedGroupId
INNER JOIN dbo.RelationshipType rel
    ON map.RelationshipTypeId = rel.Id
WHERE map.DeactivationDate IS NULL
  AND map.GroupId IN (
      -- TODO: replace with your destination system company GUIDs
      :dest_org_ids
  )
GROUP BY
    map.GroupId,
    dept.Name
ORDER BY
    map.GroupId ASC,
    dept.Name ASC;
