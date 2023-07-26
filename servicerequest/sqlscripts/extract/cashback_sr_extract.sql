select
	    ssr.request_sysid as id,
	    ssr.request_type as type,
        ssr.request_subtype as sub_type,
        ssr.description as description,
        ssr.summary as summary,
        ssr.status as status,
        ssr.substatus as sub_status,
        ssr.add_username as created_by,
        ssr.add_date as created_on,
        ssr.update_username as updated_by,
        ssr.update_date as updated_on,
        json_build_object('id', member_details.ng_member_id , 'membershipId', membership_details.ng_membership_id) as member,
        json_build_object('id', ssr.owner_username) as owner,
        json_build_object(
	        'bookingReference', ssr.bkng_refrnc_nr,
            'currency', ssr.ticket_currency,
            'customerFirstName', ssr.customer_firstname,
            'customerLastName', ssr.customer_lastname,
            'customerEmail', ssr.rqst_cb_evt_book_add_username,
			'customerPhoneNumber', ssr.mobile_num,
            'totalBookingFee', ssr.total_bkng_fee_amt,
            'postageFee', ssr.postage_amt,
            'totalActualPrice', ssr.total_price_amt,
        	'totalPriceAfterCashback', ssr.total_price_after_cb_amt,
            'totalCashback', ssr.total_price_after_cb_amt - ssr.total_price_amt,
        	'extraCashbackPercentage', ssr.extra_cashback_pct_desc,
            'extraCashback', ssr.extra_cb_amt,
        	'deliveryAddress',json_build_object(
                'addressLine1',ssr.address_1,
                'addressLine2', ssr.address_2,
                'addressLine3', ssr.address_3,
                'city', ssr.city,
                'county', ssr.county,
                'postalCode', ssr.postal_code,
                'countryName', ssr.country,
                'phoneNumber', ssr.phone_num,
                'alternatePhoneNumber', ssr.phone_num_2,
                'stage', null,
                'mobileNumber', ssr.mobile_num
            ),
        	'deliveryName',  ssr.rqst_cb_evt_book_delivery_name,
            'deliveryType',  ssr.reg_evt_bkng_ticket_delivery_type,
            'ticketProvider',  ssr.reg_evt_bkng_ticket_provider,
            'bookingDate',  ssr.rqst_cb_evt_book_add_date,
            'cancellationReason',  ssr.evnt_bkng_cancel_reason,
            'isEligibleForCashback',ssr.cb_ineligible_fl,
            'isUseMailingAddress', false,
            'isCashbackProcessed', true,
            'channel',  ssr.channel,
        	'subStatus',  ssr.substatus,
            'comments',  ssr.status_comments,
            'isLastCashbackBooking',ssr.cb_last_booking_fl,
            'onSaleRegRequestId',  ssr.onsale_reg_evt_bkng_id,
            'deliveryAddress',json_build_object(
                'toEmail',null,
                'lastName',null,
                'tenantId',null,
                'firstName',null,
                'packageId',null,
                'membershipNumber',null
            ),
            'status',ssr.status,
            'transactionAmountExcludingVat', ssr.trans_amount_excluding_vat,
            'transactionDate', ssr.transaction_date,
            'transactionPdfUrl', ssr.voucher_header_transaction_pdf_url,
            'transactionVoucherQuantity', ssr.transaction_voucher_quantity,
            'transactionTicketQty', ssr.transaction_ticket_quantity,
            'transactionTotalAmount', ssr.transaction_amount,
            'voucherSupplier', ssr.voucher_supplier,
            'deliveryEmail', ssr.rqst_cb_evt_book_add_username,
            'alternateEmail', null,
            'details', json_build_object(
                'eventName', ssr.entrtnmnt_evnt_name,
                'category', ssr.evnt_catgry_name,
                'dateTime',ssr.entrtnmnt_evnt_date,
                'seats', json_agg(json_build_object(
                    'block', null,
                    'row', null,
                    'seat', null,
                    'additionalDetails', ssr.seat_details_txt
                    )
                )
            ),
            'preferences', json_agg(json_build_object( // there are 5 columns with preference_2/3/4/5
                'order', 1,
                'venue', null,
                'timing', null,
                'additionalInfo', json_build_object(
                    'notes',ssr.preference_1,
                )
            )),
        	'venue', ssr.venue_other_name,
            'maxPrice',ssr.max_ticket_price,
            'minPrice',ssr.min_ticket_price,
            'onSaleDate',ssr.evnt_onsale_date,
            'preferredPrice',ssr.pref_ticket_price,
            'preferredSeating',ssr.preference_notes,
            'specialRequirements',ssr.special_reqs,
            'preRegistrationFlag', ssr.rqst_cb_evt_book_preregistration_fl,
            'possiblePerformances',ssr.possible_performances,
            'commentsIntern', ssr.internal_comments,
            'receivedDate', ssr.received_date_from_partner,
            'service', 'General',
            'statusReason', ssr.reason,
            'transactionAmount', ssr.transaction_amount,
            'dateReceived', ssr.received_date,
            'eventDateTime',   ssr.complaint_add_date,
            'receivedBy', ssr.received_by,
            'replyTo', ssr.reply_to,
            'severity',ssr.severity,
            'resolutionSummary', ssr.resolution_desc,
            'units', cashbackEventBookings.details,
            'voucherDetails', voucher_details.voucher_attributes
        )
        as additionalData
    from ng_intermediate_data_store.stage_service_request ssr
    left join
        (select
                svd.voucher_header_id,
                json_agg(json_build_object(
	                'description', svd.description,
	                'ecode',svd.ecode,
	                'ecodeOnline', svd.ecode_online,
	                'expiryDate', svd.expiry_date,
	                'price', svd.price,
	                'priceExcludingVat', svd.price_exclude_vat,
	                'voucherType',svd.voucher_type
                )) as voucher_attributes from stage_voucher_detail svd
                group by svd.voucher_header_id) voucher_details
            on voucher_details.voucher_header_id = ssr.voucher_header_id
    left join
        (select
                sebl.cb_evnt_bkng_sysid,
                json_agg(json_build_object(
	                'additionalInfo', json_build_object(
	                    'notes', null,
	                    'ticketLineId', sebl.cb_evnt_bkng_line_sysid
	                ),
	                'pricePerUnit', sebl.price_per_ticket_amt,
	                'quantity',sebl.tickets_qty,
	                'totalPrice', sebl.total_line_price_amt,
	                'type', sebl.ticket_type,
	                'status',sebl.order_item_status,
	                'isAvailCashback', sebl.no_cashback_fl,
	                'bookingFee', sebl.bkng_fee_amt
                )) as details from stage_cb_evnt_bkng_line sebl
                group by sebl.cb_evnt_bkng_sysid) cashbackEventBookings
            on cashbackEventBookings.cb_evnt_bkng_sysid = ssr.cb_evnt_bkng_sysid
    left join
        (select
                sorebl.onsale_reg_evt_bkng_id,
                json_agg(json_build_object(
                'notes', sorebl.notes,
                'ticketsQty', sorebl.tickets_qty,
                'ticketType', sorebl.ticket_type,
                'dateCreated', sorebl.date_created,
                'userCreated', sorebl.user_created,
                'dateModified', sorebl.date_modified,
                'userModified', sorebl.user_modified
                )) as booking_details from stage_onsale_reg_evt_bkng_line sorebl
                group by sorebl.onsale_reg_evt_bkng_id) onsaleBookings
            on onsaleBookings.onsale_reg_evt_bkng_id = ssr.onsale_reg_evt_bkng_id
    left join
        (select
                sm.member_id,
                sm.ng_member_id
                from stage_member sm
                group by sm.member_id) member_details
            on member_details.member_id = ssr.member_id
    left join
        (select
                sms.membership_id,
                sms.ng_membership_id
                from stage_membership sms
                group by sms.membership_id) membership_details
            on membership_details.membership_id = ssr.membership_id
            where ssr.request_type = 'Book Cashback Event' limit 100

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