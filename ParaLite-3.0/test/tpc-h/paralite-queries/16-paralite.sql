drop table if exists q16_tmp;
drop table if exists supplier_tmp;

create table if not exists q16_tmp(p_brand, p_type, p_size int, ps_suppkey int)
on cloko[[001-004]]
;
create table if not exists supplier_tmp(s_suppkey int)
on cloko[[001-004]]
;


--insert into supplier_tmp
create table if not exists supplier_tmp as
select 
       s_suppkey
from 
       Supplier
where 
       s_comment not like '%Customer%Complaints%'
on cloko[[001-004]]
;
  
--insert into q16_tmp
create table if not exists q16_tmp as
select 
       p.p_brand, p.p_type, p.p_size, ps.ps_suppkey
from 
       PartSupp ps 
       join Part p 
       	    on p.p_partkey = ps.ps_partkey and p.p_brand <> 'Brand#45' 
	        and p.p_type not like 'MEDIUM POLISHED%'
       join supplier_tmp s 
            on ps.ps_suppkey = s.s_suppkey
on cloko[[001-004]]
;

select 
       p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt
from 
       (select *
	from
	     q16_tmp 
	where p_size in (49, 14, 23, 45, 19, 3, 36, 9)
       ) q16_all
group by p_brand, p_type, p_size
order by supplier_cnt desc, p_brand, p_type, p_size
;

-- the same query with the last one
-- select 
--        p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt
-- from 
--        q16_tmp 
-- where p_size in (49, 14, 23, 45, 19, 3, 36, 9)
-- group by p_brand, p_type, p_size
-- order by supplier_cnt desc, p_brand, p_type, p_size
-- ;

