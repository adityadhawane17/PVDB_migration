select ssr.*,
        c.iso_4217_code as fromcurrency,
        ct.iso_4217_code as tocurrency,
        null as member,
        null as owner,
        null as additional_data
    from ng_intermediate_data_store.stage_service_request ssr
    left join ng_intermediate_data_store.currency_conversion cc on cc.currency_conversion_id = ssr.currency_conversion_id
    left join ng_intermediate_data_store.currency_type c on c.currency_type_id  = cc.from_currency_cd
    left join ng_intermediate_data_store.currency_type ct on ct.currency_type_id = cc.to_currency_cd
    where (ssr.request_sysid > {primary_key_id} and ssr.request_sysid < 1100000000) and
        (ssr.update_date > '2020-08-06' or ssr.add_date > '2020-08-06') order by request_sysid ASC limit {limit}