select
                sms.membership_id,
                sms.ng_membership_id
                from ng_intermediate_data_store.stage_membership sms
                inner join ng_intermediate_data_store.stage_service_request ssr on sms.membership_id = ssr.membership_id
                group by sms.membership_id