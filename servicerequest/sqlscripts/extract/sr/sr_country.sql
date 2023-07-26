select
    c.country_id,
    c.country_desc,
    c.iso_3166_a3_code
    from ng_intermediate_data_store.country c
inner join ng_intermediate_data_store.stage_service_request ssr on c.country_id = ssr.country
group by c.country_id
