 SELECT
    svd.voucher_header_id,
        json_agg(json_build_object(
        'description', svd.description,
        'ecode',svd.ecode,
        'ecodeOnline', svd.ecode_online,
        'expiryDate', svd.expiry_date,
        'price', svd.price,
        'priceExcludingVat', svd.price_exclude_vat,
        'voucherType',svd.voucher_type
    )) AS voucher_attributes
   FROM ng_intermediate_data_store.stage_voucher_detail svd
 inner join ng_intermediate_data_store.stage_service_request ssr on svd.voucher_header_id = ssr.voucher_header_id
 GROUP BY svd.voucher_header_id