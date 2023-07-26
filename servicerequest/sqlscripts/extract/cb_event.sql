select
                sebl.cb_evnt_bkng_sysid,
                json_agg(json_build_object(
                'cashBackEventBookingLineId', sebl.cb_evnt_bkng_line_sysid,
                'cashBackEventBookingSysid', sebl.cb_evnt_bkng_sysid,
                'orderItemStatus',sebl.order_item_status,
                'numberOfTickets',sebl.tickets_qty,
                'perTicketPrice', sebl.price_per_ticket_amt,
                'noCashBackFlag', sebl.no_cashback_fl,
                'totalPriceLine', sebl.total_line_price_amt,
                'ticketType', sebl.ticket_type,
                'bookingFee', sebl.bkng_fee_amt
                )) as details from ng_intermediate_data_store.stage_cb_evnt_bkng_line sebl
                inner join ng_intermediate_data_store.stage_service_request ssr on sebl.cb_evnt_bkng_sysid = ssr.cb_evnt_bkng_sysid
                group by sebl.cb_evnt_bkng_sysid