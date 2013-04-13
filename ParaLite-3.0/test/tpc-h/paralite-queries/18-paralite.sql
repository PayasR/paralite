drop table if exists q18_tmp;

create table if not exists q18_tmp(l_orderkey int, t_sum_quantity real)
on cloko[[001-004]]
;

create table if not exists q18_tmp as
-- insert into q18_tmp
select 
       l_orderkey, sum(l_quantity) as t_sum_quantity
from 
       LineItem
group by l_orderkey
on cloko[[001-004]]
;

select 
       c.c_name, c.c_custkey, o.o_orderkey, 
       o.o_orderdate, o.o_totalprice
       , sum(l.l_quantity)
from 
       Customer c join Orders o 
       on c.c_custkey = o.o_custkey
       join q18_tmp t 
       on o.o_orderkey = t.l_orderkey 
         -- and t.t_sum_quantity > 300
       join LineItem l 
       on o.o_orderkey = l.l_orderkey
group by 
       c.c_name, c.c_custkey, 
       o.o_orderkey, o.o_orderdate, o.o_totalprice
order by 
       o.o_totalprice, o.o_orderdate
limit 100
;

