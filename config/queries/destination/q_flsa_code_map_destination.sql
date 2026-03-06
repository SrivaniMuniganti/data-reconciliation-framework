-- q_flsa_code_map_destination.sql
-- Destination query: FLSA Code Mapping

SELECT
    a.Id,
    a.SiteId                AS dest_site_id,
    a.FlsaCode              AS flsa_code,
    a.FlsaValue             AS flsa_value
FROM dbo.FlsaCodeMap a
WHERE a.SiteId IN (
    -- TODO: replace with your destination system site GUIDs
    :dest_site_ids
);
