create view ng_intermediate_data_store.load_servicerequest as
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
        json_build_object('benefitsExplained', ssr.benefits_explained,
                        'compensationAmount', ssr.comp_amount,
                        'compensationAmountCurrency', ssr.comp_amount_currency,
                        'dateOfLoss', ssr.loss_date,
                        'receivedDateFromPartner', ssr.received_date_from_partner,
                        'detailNotExplained', ssr.detl_not_explained,
                        'exclusionsHighlighted', ssr.excl_highlighted,
                        'featuresExplained', ssr.feat_explained,
                        'placeOfLoss', ssr.place_of_loss,
                        'product', ssr.product,
                        'complainant', ssr.complainant,
                        'complaintReason', ssr.reason,
                        'replyTo', ssr.reply_to,
                        'receivedBy', ssr.received_by,
                        'notes', ssr.preference_notes,
                        'deliveryName', ssr.rqst_cb_evt_book_delivery_name,
                        'eventBkngCancelReason', ssr.evnt_bkng_cancel_reason,
                        'deliveryEmail', ssr.rqst_cb_evt_book_add_username,
                        'eventCategoryName', ssr.evnt_catgry_name,
                        'eventDate', ssr.entrtnmnt_evnt_date,
                        'eventName', ssr.entrtnmnt_evnt_name,
                        'eventOnsaleDate', ssr.evnt_onsale_date,
                        'maxTicketPrice', ssr.max_ticket_price,
                        'minTicketPrice', ssr.min_ticket_price,
                        'deliveryAddress',json_build_object(
                                                                'address1',ssr.address_1,
                                                                'address2', ssr.address_2,
                                                                'address3', ssr.address_3,
                                                                'city', ssr.city,
                                                                'county', ssr.county,
                                                                'postalCode', ssr.postal_code,
                                                                'countryISOCode', ssr.county,
                                                                'countryName', ssr.country,
                                                                'phoneNumber', ssr.phone_num,
                                                                'phoneNumber2', ssr.phone_num_2,
                                                                'mobileNumber', ssr.mobile_num),
                        'onSaleRegEventBookingLine', onsaleBookings.booking_details,
                        'onsaleRegEventBookingPreference',json_build_object(
                                                                'address1',ssr.address_1,
                                                                'address2', ssr.address_2,
                                                                'address3', ssr.address_3,
                                                                'city', ssr.city,
                                                                'county', ssr.county,
                                                                'postalCode', ssr.postal_code,
                                                                'countryISOCode', ssr.county,
                                                                'countryName', ssr.country,
                                                                'phoneNumber', ssr.phone_num,
                                                                'phoneNumber2', ssr.phone_num_2,
                                                                'mobileNumber', ssr.mobile_num),
                        'onsaleRegEventBookingPreference',json_build_object(
                                                                'preference1',ssr.preference_1,
                                                                'preference2', ssr.preference_2,
                                                                'preference3', ssr.preference_3,
                                                                'preference4', ssr.preference_4,
                                                                'preference5', ssr.preference_5,
                                                                'dateCreated', ssr.received_date,
                                                                'userCreated', ssr.received_by,
                                                                'userModified', ssr.update_username,
                                                                'dateModified', ssr.update_date),
                        'possiblePerformances', ssr.possible_performances,
                        'preferenceNotes', ssr.preference_notes,
                        'prefTicketPrice', ssr.pref_ticket_price,
                        'preRegistrationFlag', ssr.rqst_cb_evt_book_preregistration_fl,
                        'specialRequirements', ssr.special_reqs,
                        'ticketCurrency', ssr.ticket_currency,
                        'ticketDeliveryType', ssr.reg_evt_bkng_ticket_delivery_type,
                        'ticketProviderName', ssr.reg_evt_bkng_ticket_provider,
                        'totalTicketsQty', ssr.reg_evt_bkng_total_tickets_qty,
                        'vendorEventRef', ssr.rqst_cb_evt_book_vendor_event_ref,
                        'bookCashbackEvent', json_build_object(
                            'deliveryName', ssr.rqst_cb_evt_book_delivery_name,
                            'deliveryEmailAddress', ssr.rqst_cb_evt_book_add_username,
                            'bookingDate', ssr.rqst_cb_evt_book_add_date,
                            'bookingReference', ssr.bkng_refrnc_nr,
                            'cancellationReason', ssr.evnt_bkng_cancel_reason,
                            'cashBackInEligibleFlag', ssr.cb_ineligible_fl,
                            'cashBackLastBookingFlag', ssr.cb_last_booking_fl,
                            'extraCashBackAmountPercentage', ssr.extra_cashback_pct_id,
                            'extraCashBackAmountValue', ssr.extra_cb_amt,
                            'eventCategoryName', ssr.evnt_catgry_name,
                            'eventDate', ssr.entrtnmnt_evnt_date,
                            'eventName', ssr.entrtnmnt_evnt_name,
                            'isMemberImpersonation', null,
                            'onSaleRegRequestId', ssr.onsale_reg_evt_bkng_id,
                            'currency', ssr.ticket_currency,
                            'orderRef', ssr.rqst_cb_evt_book_source_order_ref,
                            'postage', ssr.postal_code,
                            'preRegistrationFlag', ssr.rqst_cb_evt_book_preregistration_fl,
                            'purchaseDate', ssr.purchase_date,
                            'ticketdeliveryType', ssr.reg_evt_bkng_ticket_delivery_type,
                            'ticketsPrinted', null,
                            'ticketProviderName', ssr.reg_evt_bkng_ticket_provider,
                            'totalActualPrice', ssr.total_price_amt,
                            'totalBookingFee', ssr.total_bkng_fee_amt,
                            'totalNumOfTickets', ssr.transaction_ticket_quantity,
                            'totalPriceAfterCashBack', ssr.total_price_after_cb_amt,
                            'vendorEventRef', ssr.rqst_cb_evt_book_vendor_event_ref,
                            'venueCityName', ssr.venue_city,
                            'venueName', ssr.venue_id,
                            'venueNameOther', ssr.venue_other_name,
                            'cashBackId', ssr.cb_evnt_bkng_sysid,
                            'paymentVehicleId', null,
                            'seatReferences', ssr.seat_details_txt,
                            'payment',json_build_object(
                                                                'billId',null,
                                                                'parentBillId', null,
                                                                'billStatus', null,
                                                                'statusDate', null),
                            'deliveryAddress',json_build_object(
                                                                'address1',ssr.address_1,
                                                                'address2', ssr.address_2,
                                                                'address3', ssr.address_3,
                                                                'city', ssr.city,
                                                                'county', ssr.county,
                                                                'postalCode', ssr.postal_code,
                                                                'countryName', ssr.country,
                                                                'phoneNumber', ssr.phone_num,
                                                                'phoneNumber2', ssr.phone_num_2,
                                                                'mobileNumber', ssr.mobile_num),
                            'cashBackEventBookingLine', cashbackEventBookings.details
                            ),
                        'cinemaStoreVoucher', json_build_object(
                            'customerFirstName', ssr.customer_firstname,
                            'customerLastName', ssr.customer_lastname,
                            'customerEmail', ssr.voucher_header_customer_email,
                            'voucherSupplier', ssr.voucher_supplier,
                            'transactionTotalAmount', ssr.transaction_amount,
                            'transactionAmountExcludingVat', ssr.trans_amount_excluding_vat,
                            'transactionVoucherQuantity', ssr.transaction_voucher_quantity,
                            'transactionTicketQuantity', ssr.transaction_ticket_quantity,
                            'transactionDate', ssr.transaction_date,
                            'currencyIsoCode', null,
                            'transactionPdfUrl', ssr.voucher_header_transaction_pdf_url,
                            'sourceOrderReference', ssr.voucher_header_source_order_ref,
                            'dateCreated', ssr.received_date,
                            'userCreated', ssr.received_by,
                            'userModified', ssr.update_username,
                            'cinemaStoreVoucherDetailId', voucher_details.voucher_attributes,
                            'dateModified', ssr.update_date),
                        'cost', ssr.transaction_amount,
                        'eventDate', ssr.entrtnmnt_evnt_date,
                        'fulfillmentDate', ssr.activation_date,
                        'numberOfParticipants', null,
                        'supplier', ssr.voucher_supplier
                        )
        as additionalData
    from ng_intermediate_data_store.stage_service_request ssr
    left join
        (select
                svd.voucher_header_id,
                json_agg(json_build_object(
                'cinemaStoreVoucherDetailId', svd.voucher_detail_id,
                'voucher_header_id', svd.voucher_header_id,
                'voucherType',svd.voucher_type,
                'ecode',svd.ecode,
                'ecodeOnline', svd.ecode_online,
                'expiryDate', svd.expiry_date,
                'cost', svd.cost,
                'costExcludingVat', svd.cost_exclude_vat,
                'price', svd.price,
                'priceExcludingVat', svd.price_exclude_vat,
                'description', svd.description,
                'extraAttribute', svd.extra_attribute,
                'loadDate', svd.load_date,
                'loadFilename', svd.load_filename,
                'loadFileSupplier', svd.load_file_supplier,
                'dateCreated', svd.date_created,
                'userCreated', svd.user_created,
                'dateModified', svd.date_modified,
                'userModified', svd.user_modified
                )) as voucher_attributes from stage_voucher_detail svd
                group by svd.voucher_header_id) voucher_details
            on voucher_details.voucher_header_id = ssr.voucher_header_id
    left join
        (select
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
;