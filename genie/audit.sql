-- Example: who created/updated spaces and asked questions (workspace-level)
SELECT event_time, service_name, action_name, request_params, user_identity
FROM system.audit.logs
WHERE service_name IN ('aibiGenie','dashboards')
ORDER BY event_time DESC
LIMIT 100;
