select
                sorebl.onsale_reg_evt_bkng_id,
                json_agg(json_build_object(
                'notes', sorebl.notes,
                'ticketsQty', sorebl.tickets_qty,
                'ticketType', sorebl.ticket_type,
                'dateCreated', sorebl.date_created,
                'userCreated', sorebl.user_created,
                'dateModified', sorebl.date_modified,
                'userModified', sorebl.user_modified
                )) as booking_details from ng_intermediate_data_store.stage_onsale_reg_evt_bkng_line sorebl
                inner join ng_intermediate_data_store.stage_service_request ssr on sorebl.onsale_reg_evt_bkng_id = ssr.onsale_reg_evt_bkng_id
                group by sorebl.onsale_reg_evt_bkng_id