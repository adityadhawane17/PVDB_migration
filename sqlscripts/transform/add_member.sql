with mem as (
insert into migration."member"
(
    external_ref_id,
    prefix,first_name,
    middle_name,
    last_name,
    suffix,
    salutation,
    gender,
    date_of_birth,
    status,active_from,
    active_to,
    is_active,
    is_primary,
    is_web_enabled,
    attributes,
    user_created,
    date_created,
    user_modified,
    date_modified,
    member_number,
    member_join_date)
    (select external_ref_id,prefix,first_name,middle_name,last_name,
    suffix,salutation,gender,date_of_birth,status,active_from,active_to,
    is_active,is_primary,is_web_enabled,attributes,user_created,
    date_created,user_modified,date_modified,member_number,member_join_date
    from migration.load_member_from_stage_tmp lmfst)
    returning id,external_ref_id
)
insert into migration."address" (
    member_id,address_type,address_line1,address_line2,city_name,
    state_code,country_name,country_code,postal_code,user_created,
    date_created,user_modified,date_modified,county
    )
    (select mem.id as member_id,address_type,address_line1,address_line2,city_name,
     state_code,country_name,country_code, postal_code,address_user_created,
     address_date_created,address_user_modified,address_date_modified,county address_line1
     from migration.load_member_from_stage_tmp
     join mem on mem.external_ref_id = migration.load_member_from_stage_tmp.external_ref_id);