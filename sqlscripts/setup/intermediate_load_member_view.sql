create view ng_intermediate_data_store.load_address as
	select
	    m.id as id,
		m.external_ref_id as external_ref_id,
		sm.title_desc  as prefix,
		sm.fname as first_name,
		sm.mname as middle_name,
		sm.lname as last_name,
		sm.suffix as suffix,
		sm.salutation as salutation,
		sm.mem_gender_name as gender,
		sm.dob as date_of_birth,
		ng_intermediate_data_store.member_status(is_active) as status,
		sm.created_date as active_from,
		sm.is_active as is_active,
		smjoined_attributes.attributes
		from  ng_intermediate_data_store.stage_member sm
		left join
		(select
			sm.member_id,
			jsonb_object_agg(sma.member_attribute_type_desc ,sma.member_attribute_value) as attributes
		 from ng_intermediate_data_store.stage_member sm
		 inner join ng_intermediate_data_store.stage_member_attribute sma
		 on sm.member_id = sma.member_id group by sm.member_id
		 ) smjoined_attributes
		on smjoined_attributes.member_id = sm.member_id
		order by external_ref_id
	;