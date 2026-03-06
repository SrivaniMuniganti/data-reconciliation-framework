-- q_time_off_reasons_destination.sql
-- Destination query: Time-Off Reason Types

SELECT
    GroupId         AS dest_org_id,
    Value           AS reason_value,
    Active          AS is_active
FROM dbo.TimeOffReason
WHERE GroupId IN (
    -- TODO: replace with your destination system company GUIDs
    :dest_org_ids
);
