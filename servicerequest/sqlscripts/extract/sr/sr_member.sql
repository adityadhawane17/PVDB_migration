select
	sm.member_id,
	sm.ng_member_id,
	sm.primary_email_address
from
	ng_intermediate_data_store.stage_member sm
inner join ng_intermediate_data_store.stage_service_request ssr on
	sm.member_id = ssr.member_id
group by
	sm.member_id