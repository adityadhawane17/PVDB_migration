select
    ssr.request_sysid as id,
    json_build_object('id', member_details.ng_member_id , 'membershipId', membership_details.ng_membership_id) as member
    from (select request_sysid, membership_id, member_id from ng_intermediate_data_store.stage_service_request where request_sysid > {primary_key_id} order by request_sysid limit {limit}) ssr
        left join
                (select
                        sm.member_id,
                        sm.ng_member_id
                        from ng_intermediate_data_store.stage_member sm
                        group by sm.member_id) member_details
                    on member_details.member_id = ssr.member_id
        left join
            (select
                    sms.membership_id,
                    sms.ng_membership_id
                    from ng_intermediate_data_store.stage_membership sms
                    group by sms.membership_id) membership_details
                on membership_details.membership_id = ssr.membership_id