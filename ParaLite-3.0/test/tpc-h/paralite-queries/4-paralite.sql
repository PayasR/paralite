drop table if exists q4_order_priority_tmp;

create table if not exists q4_order_priority_tmp (o_orderkey INT)
on cloko[[001-004]]
;

create table if not exists q4_order_priority_tmp as
-- -- --insert into q4_order_priority_tmp
      select DISTINCT 
            l_orderkey 
	 from 
	       LineItem
	 where 
	       l_commitdate < l_receiptdate
on cloko[[001-004]]
;

select 
       o.o_orderpriority, count(*) as order_count 
from 
       Orders o
       join q4_order_priority_tmp t 
       on o.o_orderkey = t.o_orderkey and
	  o.o_orderdate >= '1993-07-01' 
	  and o.o_orderdate < '1993-10-01' 
group by 
       o.o_orderpriority 
order by 
       o.o_orderpriority
;

