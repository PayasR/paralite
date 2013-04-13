--Unsupported!

-- drop table if exists q11_part_tmp;
-- drop table if exists q11_sum_tmp;

-- create table if not exists q11_part_tmp(ps_partkey int, part_value real)
-- on cloko[[001-004]]
-- ;

-- create table if not exists q11_sum_tmp(total_value real)
-- on cloko[[001-004]]
-- ;

-- create table if not exists q11_part_tmp as 
-- --insert into q11_part_tmp
-- select 
--        PS.ps_partkey, sum(PS.ps_supplycost * PS.ps_availqty) as part_value 
-- from
--        Nation N 
--        join Supplier S on 
--            S.s_nationkey = N.n_nationkey and N.n_name = 'ARGENTINA'
--        join PartSupp PS on  
--            PS.ps_suppkey = S.s_suppkey
-- group by PS.ps_partkey
-- on cloko[[001-004]]
-- ;

-- create table if not exists q11_sum_tmp as
-- --insert into q11_sum_tmp
-- select 
--   sum(part_value) as total_value
-- from 
--   q11_part_tmp
-- on cloko[[001-004]]
-- ;

select 
  ps_partkey, part_value as value1
from
  (
    select t1.ps_partkey as ps_partkey, t1.part_value as part_value, 
    	   t2.total_value as total_value
    from q11_part_tmp t1 join q11_sum_tmp t2
  ) a
where part_value > total_value * 0.0001
order by value1 desc
;
