select
    sebl.discnt_evnt_bkng_sysid,
    json_agg(json_build_object(
        'additionalInfo', json_build_object(
            'notes', '',
            'ticketLineId', sebl.discnt_evnt_bkng_sysid
        ),
        'pricePerUnit', sebl.price_per_ticket_amt,
        'quantity',sebl.tickets_qty,
        'totalPrice', sebl.total_line_price_amt,
        'type', lv.value ,
        'status',ls.value ,
        'isAvailDiscount', (case sebl.no_discount_flag when 'N' then true else false end),
        'bookingFee', sebl.bkng_fee_amt,
        'autoDiscountPercentageApplied', sebl.auto_discnt_pct,
        'totalPriceAfterDiscount', sebl.total_price_after_discnt_amt
    )) as details from ng_intermediate_data_store.stage_discnt_evnt_bkng_line sebl
left join ng_intermediate_data_store.stage_service_request ssr on sebl.discnt_evnt_bkng_sysid = ssr.discnt_evnt_bkng_sysid
left join ng_intermediate_data_store.lov_value lv on lv.lov_value_sysid = sebl.ticket_type_id
left join ng_intermediate_data_store.lov_value ls on ls.lov_value_sysid = sebl.order_item_status_id
group by sebl.discnt_evnt_bkng_sysid
