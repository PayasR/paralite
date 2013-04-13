drop table if exists q2_minimum_cost_supplier_tmp1;
drop table if exists q2_minimum_cost_supplier_tmp2;


create table if not exists q2_minimum_cost_supplier_tmp1 (
       s_acctbal real, s_name, n_name, p_partkey int, 
       ps_supplycost real, p_mfgr, s_address, s_phone, s_comment) 
on cloko[[001-004]]
;

create table if not exists q2_minimum_cost_supplier_tmp2 (
       p_partkey int, ps_min_supplycost real) 
on cloko[[001-004]]
; 


create table if not exists q2_minimum_cost_supplier_tmp1 as
       select s.s_acctbal, s.s_name, n.n_name, p.p_partkey, ps.ps_supplycost, 
              p.p_mfgr, s.s_address, s.s_phone, s.s_comment         
       from  Nation n 
       	     join Region r
	         on n.n_regionkey = r.r_regionkey and r.r_name = 'ASIA'  
	     join Supplier s  
	     	 on s.s_nationkey = n.n_nationkey
             join PartSupp ps
	         on s.s_suppkey = ps.ps_suppkey 
  	     join Part p  
 	         on p.p_partkey = ps.ps_partkey and p.p_size = 27
		    and p.p_type like '%LARGE%' 
on cloko[[001-004]]
;

create table if not exists q2_minimum_cost_supplier_tmp2 as
       select p_partkey, min(ps_supplycost)
       from q2_minimum_cost_supplier_tmp1 
       group by p_partkey
on cloko[[001-004]]
; 


select 
     t1.s_acctbal, t1.s_name, t1.n_name, t1.p_partkey, t1.p_mfgr, t1.s_address, 
     t1.s_phone, t1.s_comment 
from 
     q2_minimum_cost_supplier_tmp1 t1 
     join q2_minimum_cost_supplier_tmp2 t2 
     	  on t1.p_partkey = t2.p_partkey and t1.ps_supplycost=t2.ps_min_supplycost 
order by t1.s_acctbal desc, t1.n_name, t1.s_name, t1.p_partkey 
limit 100
;
