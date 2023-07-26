select
	sorebl.onsale_reg_evt_bkng_id,
	json_agg(json_build_object(
	    'additionalInfo', json_build_object(
	        'notes', (case when sorebl.notes is null then '' else sorebl.notes end),
	        'ticketLineId', sorebl.onsale_reg_evt_bkng_line_id
	        ),
        'pricePerUnit', '',
        'quantity', sorebl.tickets_qty,
        'totalPrice', '',
        'type', sorebl.ticket_type,
        'status', '',
        'isAvailCashback', false,
        'bookingFee', '')
     ) as booking_details
from
	ng_intermediate_data_store.stage_onsale_reg_evt_bkng_line sorebl
inner join ng_intermediate_data_store.stage_service_request ssr on
	sorebl.onsale_reg_evt_bkng_id = ssr.onsale_reg_evt_bkng_id
group by
	sorebl.onsale_reg_evt_bkng_id