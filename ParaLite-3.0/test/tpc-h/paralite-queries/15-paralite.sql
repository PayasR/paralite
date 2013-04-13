drop table if exists revenue;
drop table if exists max_revenue;

create table if not exists revenue(supplier_no int, total_revenue real)
on cloko[[001-004]]
;
 
create table if not exists max_revenue(max_revenue real)
on cloko[[001-004]]
;
 
create table if not exists revenue as 
--insert into revenue
select 
       l_suppkey as supplier_no, 
       sum(l_extendedprice * (1 - l_discount)) as total_revenue
from 
       LineItem
where 
       l_shipdate >= '1996-01-01' and l_shipdate < '1996-04-01'
group by l_suppkey
on cloko[[001-004]]
;

create table if not exists max_revenue as
--insert into max_revenue
select 
     max(total_revenue)
from 
     revenue
on cloko[[001-004]]
;

select 
     s.s_suppkey, s.s_name, s.s_address, s.s_phone, r.total_revenue
from Supplier s 
     join revenue r 
     	  on s.s_suppkey = r.supplier_no
     join max_revenue m 
       	  on r.total_revenue = m.max_revenue
order by s.s_suppkey
;

