-- q_feature_flags_destination.sql
-- Destination query: Feature Flags per Site
-- Replace the FeatureId GUID with the GUID for the feature you are validating.

SELECT
    GroupId         AS dest_site_id,
    IsEnabled       AS feature_flag
FROM FeatureFlagEnabled
WHERE FeatureId = :feature_guid
  AND GroupId IN (
      -- TODO: replace with your destination system site GUIDs
      :dest_site_ids
  );
