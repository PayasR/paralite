-- drop table if exists q20_tmp1;
-- drop table if exists q20_tmp2;
-- drop table if exists q20_tmp3;
-- drop table if exists q20_tmp4;

create table if not exists q20_tmp1(p_partkey int)
on cloko[[100-109]] cloko[[111-112]] cloko[[114-117]]
;
create table if not exists q20_tmp2(l_partkey int, l_suppkey int, sum_quantity real)
on cloko[[100-109]] cloko[[111-112]] cloko[[114-117]]
;
create table if not exists q20_tmp3(ps_suppkey int, ps_availqty int, sum_quantity real)
on cloko[[100-109]] cloko[[111-112]] cloko[[114-117]]
;
create table if not exists q20_tmp4(ps_suppkey int)
on cloko[[100-109]] cloko[[111-112]] cloko[[114-117]]
;

---10s

create table if not exists q20_tmp1 as
--insert into q20_tmp1
select distinct p_partkey
from
	Part 
where 
        p_name like 'forest%'
on cloko[[100-109]] cloko[[111-112]] cloko[[114-117]]
;


--162s  (103 load data)
create table if not exists q20_tmp2 as
-- insert into q20_tmp2
select 
        l_partkey, l_suppkey, 0.5 * sum(l_quantity)
from
	LineItem
where
	l_shipdate >= '1994-01-01'
	and l_shipdate < '1995-01-01'
group by l_partkey, l_suppkey
on cloko[[100-109]] cloko[[111-112]] cloko[[114-117]]
;

---63s (10 load data)
create table if not exists q20_tmp3 as
--insert into q20_tmp3
select 
       ps.ps_suppkey, ps.ps_availqty, t2.sum_quantity
from  
       PartSupp ps join q20_tmp1 t1 
       on ps.ps_partkey = t1.p_partkey
       join q20_tmp2 t2 
       on ps.ps_partkey = t2.l_partkey and ps.ps_suppkey = t2.l_suppkey
on cloko[[100-109]] cloko[[111-112]] cloko[[114-117]]
;


--9s
create table if not exists q20_tmp4 as
-- insert into q20_tmp4
select 
       ps_suppkey
from 
       q20_tmp3
where 
       ps_availqty > sum_quantity
group by ps_suppkey
on cloko[[100-109]] cloko[[111-112]] cloko[[114-117]]
;


--63s
select 
       s.s_name, s.s_address
from 
       Supplier s join Nation n
       on s.s_nationkey = n.n_nationkey
          and n.n_name = 'CANADA'
       join q20_tmp4 t4
       on s.s_suppkey = t4.ps_suppkey
order by s.s_name
;


