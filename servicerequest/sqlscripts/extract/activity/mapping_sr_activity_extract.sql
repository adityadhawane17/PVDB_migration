SELECT
	request_sysid AS service_request_id,
	activity_sysid AS activities_id
FROM
	ng_intermediate_data_store.stage_service_activity
WHERE
	activity_sysid > {primary_key_id}
ORDER BY
	activity_sysid
LIMIT {limit}