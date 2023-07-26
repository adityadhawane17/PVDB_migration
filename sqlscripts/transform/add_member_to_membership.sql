truncate table migration."member";

insert into migration."member" (id,is_web_enabled,external_ref_id)
select *
from dblink('dbname=member hostaddr=10.222.113.196 user=member
password=xQvv]:s$js{vME8m',
'select id,is_web_enabled,external_ref_id from migration.member') as t1(id uuid, is_web_enabled bool,external_ref_id varchar);