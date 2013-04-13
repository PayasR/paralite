-- $ID$
-- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
-- Functional Query Definition
-- Approved February 1998
select
	P.p_brand,
	P.p_type,
	P.p_size,
	count( PS.ps_suppkey) as supplier_cnt
from
	PartSupp PS,
	Part P
where
	P.p_partkey = PS.ps_partkey
	and P.p_brand <> 'Brand#42'
	and P.p_type not like 'SMALL%'
	and P.p_size in ('PROMO BURNISHED COPPER', 'LARGE BRUSHED BRASS', 'STANDARD POLISHED BRASS', 'SMALL PLATED BRASS', 'STANDARD POLISHED TIN', 'PROMO PLATED STEEL', 'SMALL PLATED COPPER', 'PROMO BURNISHED TIN')
	and PS.ps_suppkey not in (
		select
			S.s_suppkey
		from
			Supplier S
		where
			S.s_comment like '%Customer%Complaints%'
	)
group by
	P.p_brand,
	P.p_type,
	P.p_size
order by
	supplier_cnt desc,
	P.p_brand,
	P.p_type,
	P.p_size
limit 100;
