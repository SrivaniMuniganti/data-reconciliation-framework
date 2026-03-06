-- q_employees_destination.sql
-- Destination query: Employee / Person records

SELECT
    pgm.HomeSiteId          AS dest_site_id,
    p.Email                 AS email,
    CONCAT(p.FirstName, ' ', p.LastName) AS full_name,
    p.Id                    AS person_id,
    COUNT(DISTINCT p.Id)    AS record_count
FROM Person p
LEFT JOIN [User] u
    ON p.Id = u.PersonId
LEFT JOIN PersonSiteMap pgm
    ON p.Id = pgm.PersonId
LEFT JOIN PersonPermission pp
    ON p.Id = pp.PersonId
LEFT JOIN [OrgGroup] g_home
    ON pgm.HomeSiteId = g_home.Id
LEFT JOIN OrgGroupMap gm
    ON pgm.GroupId = gm.GroupId
LEFT JOIN [OrgGroup] g_company
    ON gm.GroupId = g_company.Id
WHERE
    pgm.HomeSiteId IN (
        -- TODO: replace with your destination system site GUIDs
        :dest_site_ids
    )
GROUP BY
    pgm.HomeSiteId,
    p.Email,
    pgm.ExternalId,
    p.FirstName,
    p.LastName,
    p.Id
ORDER BY
    pgm.HomeSiteId ASC,
    p.Email ASC;
