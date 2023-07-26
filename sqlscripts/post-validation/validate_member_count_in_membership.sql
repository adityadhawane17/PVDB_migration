SELECT
(SELECT count(*) from migration."member" m )=
(select *
from dblink('dbname=member hostaddr=10.222.113.196 user=member
password=*********,
'select count(*) from migration.member') as t1(count int))
AS RowCountResult;
