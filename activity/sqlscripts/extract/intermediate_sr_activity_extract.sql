select
	    activity_sysid as id,
	    request_sysid as sr_id,
	    activity_type as type,
        activity_subtype as sub_type,
        name as summary,
        status as status,
        priority as priority,
        comm_channel as channel,
        add_date as created_on,
        add_username as created_by,
        update_date as updated_on,
        update_username as updated_by,
        json_build_object(
                'protected_card_sysid', protected_card_sysid,
                'bank_account_sysid', bank_account_sysid,
                'generic_asset_sysid', generic_asset_sysid,
                'bank_branch_sysid', bank_branch_sysid,
                'planned_start_date', planned_start_date,
                'actual_start_date', actual_start_date,
                'req_completion_date', req_completion_date,
                'actual_completion_date', actual_completion_date,
                'comments', comments,
                'contact_method', contact_method,
                'duplicate_touchpoint_flag', duplicate_touchpoint_flag,
                'email_address', email_address,
                'fax_numeric', fax_numeric,
                'new_protected_card_sysid', new_protected_card_sysid,
                'new_bank_account_sysid', new_bank_account_sysid,
                'new_generic_asset_sysid', new_generic_asset_sysid,
                'phone_numeric', phone_numeric,
                'sla_target1', sla_target1,
                'sla_target2', sla_target2,
                'status_date', status_date,
                'touchpoint_flag', touchpoint_flag,
                'ext_activity_id', ext_activity_id,
                'activity_lstcrd_sysid', activity_lstcrd_sysid,
                'issuer_contact_sysid', issuer_contact_sysid,
                'lost_stolen_card_sysid', lost_stolen_card_sysid,
                'loss_center_comments', loss_center_comments,
                'activity_pmt_sysid', activity_pmt_sysid,
                'payment_sysid', payment_sysid,
                'payment_vehicle_sysid', payment_vehicle_sysid,
                'flow_type', flow_type,
                'amount', amount,
                'currency', currency,
                'payment_status_sysid', payment_status_sysid,
                'activity_payment_add_date', activity_payment_add_date,
                'activity_payment_add_username', activity_payment_add_username,
                'activity_payment_update_date', activity_payment_update_date,
                'activity_payment_update_username', activity_payment_update_username,
                'eft_description', eft_description,
                'payment_comments', payment_comments,
                'activity_corr_sysid', activity_corr_sysid,
                'legacy_template', legacy_template,
                'comm_transaction_id', comm_transaction_id,
                'corr_free_text', corr_free_text,
                'activity_corr_add_date', activity_corr_add_date,
                'activity_corr_add_username', activity_corr_add_username,
                'activity_corr_update_date', activity_corr_update_date,
                'activity_corr_update_username', activity_corr_update_username,
                'comm_type_id', comm_type_id,
                'kit_id', kit_id
            )
        as additionalData
    from ng_intermediate_data_store.stage_service_activity
    where (activity_sysid > {primary_key_id} and activity_sysid < 1100000000) and
    (update_date > '2020-08-06' or add_date > '2020-08-06')
    order by activity_sysid limit {limit}