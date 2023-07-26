select
    spv.protected_card_sysid,
    json_agg(json_build_object(
    'amount', '',
    'billId', '',
    'flowType', 'debit',
    'isActive', false,
    'createdBy', spv.add_username,
    'createdOn', spv.add_date,
    'billStatus', '',
    'modifiedBy', spv.updated_username,
    'modifiedOn', spv.update_date,
    'statusDate', spv.update_date,
    'paymentType', isr.name,
    'paymentMethod', spv.payment_vehicle_type_desc,
    'paymentVehicleId', spv.payment_vehicle_object_id,
    'transactionReason', ''
    )) as payment_vehicle_details from ng_intermediate_data_store.stage_payment_vehicle spv
 inner join ng_intermediate_data_store.stage_service_request ssr on spv.protected_card_sysid = ssr.protected_card_sysid
 inner join ng_intermediate_data_store.stage_protected_card spc on spv.protected_card_sysid = spc.protected_card_sysid
 inner join ng_intermediate_data_store.issuer isr on isr.issuer_sysid = spc.issuer_sysid
 GROUP BY spv.protected_card_sysid