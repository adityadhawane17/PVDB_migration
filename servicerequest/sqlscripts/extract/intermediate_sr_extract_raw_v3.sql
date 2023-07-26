select *,
       c.iso_4217_code as fromcurrency,
       cA.iso_4217_code as tocurrency,
       null as member,
       null as owner,
       null as additional_data
    from ng_intermediate_data_store.stage_service_request ssr
    left join ng_intermediate_data_store.currency_conversion cc on cc.currency_conversion_id = ssr.currency_conversion_id
    left join ng_intermediate_data_store.currency_type c on c.currency_type_id  = cc.from_currency_cd
    left join ng_intermediate_data_store.currency_type cA on ca.currency_type_id = cc.to_currency_cd
    where request_type = 'Book Cashback Event'
