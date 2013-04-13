-- drop table if exists lineitem_tmp;

-- create table if not exists lineitem_tmp (t_partkey int, t_avg_quantity real)
-- on cloko[[001-004]]
-- ;

-- create table if not exists lineitem_tmp as
-- --insert into lineitem_tmp
-- select 
--        l_partkey as t_partkey, 0.2 * avg(l_quantity) as t_avg_quantity
-- from 
--        LineItem
-- group by l_partkey
-- on cloko[[001-004]]
-- ;

-- select
-- 	sum(l_extendedprice) / 7.0 as avg_yearly
-- from
-- 	(select 
-- 		l_quantity, l_extendedprice, t_avg_quantity 
-- 	 from
-- 		LineItem_tmp t join
-- 		(select
-- 			l_quantity, l_partkey, l_extendedprice
-- 		 from
-- 		        Part p join LineItem l
-- 			        on p.p_partkey = l.l_partkey
-- 				   --and p.p_brand = 'Brand#23'
-- 				   and p.p_container = 'MED BOX'
-- 		) l1 on l1.l_partkey = t.t_partkey
--    	) a
-- where l_quantity < t_avg_quantity
-- ;

select
	sum(l.l_extendedprice) / 7.0 as avg_yearly
from
	Part p, LineItem l
	, lineitem_tmp t 
where 
        p.p_partkey = l.l_partkey
	--and p.p_brand = 'Brand#23'
	and p.p_container = 'MED BOX'
	and l.l_partkey = t.t_partkey
	and l.l_quantity < t.t_avg_quantity
;


