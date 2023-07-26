select ssr.*,
        null as member,
        null as owner,
        null as additional_data
    from ng_intermediate_data_store.stage_service_request ssr
    where ssr.request_sysid > {primary_key_id}  and ssr.request_type = 'Book Event'
      order by request_sysid ASC limit {limit}