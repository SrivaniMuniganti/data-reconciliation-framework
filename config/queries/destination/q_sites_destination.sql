-- q_sites_destination.sql
-- Destination query: Sites / Communities

SELECT
    gc.Id           AS dest_site_id,
    gc.Name         AS site_name,
    p.Name,
    p.Abbreviation,
    p.TimeZone,
    a.Street,
    a.City,
    a.State,
    a.Zip,
    a.PhoneNumber
FROM [OrgGroup] gco
JOIN OrgGroupType gtco
    ON gtco.Id = gco.OrgGroupTypeId AND gtco.Name = 'Company'
JOIN OrgGroupMap gmcoc ON gmcoc.GroupId = gco.Id
JOIN RelationshipType rtcoc ON rtcoc.Id = gmcoc.RelationshipTypeId
JOIN [OrgGroup] gc ON gc.Id = gmcoc.RelatedGroupId
JOIN OrgGroupType gtc ON gtc.Id = gc.OrgGroupTypeId AND gtc.Name = 'Site'
JOIN SiteLocationMap gp ON gp.SiteId = gc.Id
JOIN Location p ON p.Id = gp.LocationId
JOIN LocationType pt ON pt.Id = p.LocationTypeId AND pt.Name = 'Building'
JOIN Address a ON a.Id = p.AddressId
WHERE gco.Id IN (
    -- TODO: replace with your destination system company GUIDs
    :dest_org_ids
);
