select
    sebl.cb_evnt_bkng_sysid,
    json_agg(json_build_object(
    'additionalInfo', json_build_object(
        'notes', '',
        'ticketLineId', sebl.cb_evnt_bkng_line_sysid
    ),
    'pricePerUnit', sebl.price_per_ticket_amt,
    'quantity',sebl.tickets_qty,
    'totalPrice', sebl.total_line_price_amt,
    'type', sebl.ticket_type,
    'status',sebl.order_item_status,
    'isAvailCashback', (case sebl.no_cashback_fl when 1 then false else true end),
    'bookingFee', sebl.bkng_fee_amt
    )) as details from ng_intermediate_data_store.stage_cb_evnt_bkng_line sebl
    inner join ng_intermediate_data_store.stage_service_request ssr on sebl.cb_evnt_bkng_sysid = ssr.cb_evnt_bkng_sysid
    group by sebl.cb_evnt_bkng_sysid