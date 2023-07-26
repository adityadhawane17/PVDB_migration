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
                'countryName', country.country_desc,
                'countryISOCode', country.iso_3166_a3_code,
                'phoneNumber', ssr.phone_num,
                'alternatePhoneNumber', ssr.phone_num_2,
                'state', null,
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
            'notification',json_build_object(
                'toEmail', null,
                'lastName', null,
                'tenantId', null,
                'firstName', null,
                'packageId', null,
                'membershipNumber', null
            ),
            'status', ssr.status,
            'transactionAmountExcludingVat',ssr.trans_amount_excluding_vat,
            'transactionDate', ssr.transaction_date,
            'transactionPdfUrl',ssr.voucher_header_transaction_pdf_url,
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
                'venue', ssr.venue_other_name,
                'seats', json_agg(json_build_object(
                    'block', null,
                    'row', null,
                    'seat', null,
                    'additionalDetails', ssr.seat_details_txt
                    )
                )
            ),
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
            'thirdPartyPhone', null,
            'thirdParty', null,
            'transactionAmount', ssr.transaction_amount,
            'compensationAmount',  ssr.comp_amount,
            'compensationType',    ssr.compensation_type,
            'complainant', ssr.complainant,
            'complaintDetail',null,
            'complaintNature', null,
            'complaintReason', ssr.reason,
            'complaintResolution', ssr.resolution_desc,
            'dateReceived', ssr.received_date,
            'eventDateTime',   ssr.complaint_add_date,
            'receivedBy', ssr.received_by,
            'replyTo', ssr.reply_to,
            'severity',ssr.severity,
            'thirdPartyNotes',null,
            'resolutionSummary', ssr.resolution_desc,
            'units', cashbackEventBookings.details,
            'voucherDetails', voucher_details.voucher_attributes,
            'payments',paymentVehile.payment_vehicle_details
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
                )) as voucher_attributes from ng_intermediate_data_store.stage_voucher_detail svd
                group by svd.voucher_header_id) voucher_details
            on voucher_details.voucher_header_id = ssr.voucher_header_id
    left join
        (select
                sebl.cb_evnt_bkng_sysid,
                json_agg(json_build_object(
                'additionalInfo', json_build_object(
                    'notes', null,
                    'ticketLineId', sebl.cb_evnt_bkng_sysid
                ),
                'pricePerUnit', sebl.price_per_ticket_amt,
                'quantity',sebl.tickets_qty,
                'totalPrice', sebl.total_line_price_amt,
                'type', sebl.ticket_type,
                'status',sebl.order_item_status,
                'isAvailCashback', sebl.no_cashback_fl,
                'bookingFee', sebl.bkng_fee_amt
                )) as details from ng_intermediate_data_store.stage_cb_evnt_bkng_line sebl
                group by sebl.cb_evnt_bkng_sysid) cashbackEventBookings
            on cashbackEventBookings.cb_evnt_bkng_sysid = ssr.cb_evnt_bkng_sysid
    left join
        (select
                spv.protected_card_sysid,
                json_agg(json_build_object(
                'amount', null,
                'billId', null,
                'flowType', 'debit',
                'isActive', true,
                'createdBy', spv.add_username,
                'createdOn', spv.add_date,
                'billStatus', null,
                'modifiedBy', spv.updated_username,
                'modifiedOn', spv.update_date,
                'statusDate', spv.update_date,
                'paymentType',null,
                'paymentMethod', spv.payment_vehicle_type_desc,
                'paymentVehicleId', spv.payment_vehicle_object_id,
                'transactionReason', 'Incentive'
                )) as payment_vehicle_details from ng_intermediate_data_store.stage_payment_vehicle spv
                group by spv.protected_card_sysid) paymentVehile
            on paymentVehile.protected_card_sysid = ssr.protected_card_sysid
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
    left join
        (select
            c.country_id,
            c.country_desc,
            c.iso_3166_a3_code
            from country c
            group by c.country_id) country
            on country.country_id = ssr.country
    left join
        (SELECT
            i.issuer_sysid,
            i.issuer_name
            FROM ng_intermediate_data_store.issuer i
            group by i.issuer_sysid) issuer
            on issuer.issuer_sysid = ssr.issuer_sysid
    where ssr.request_type = 'Book Cashback Event'