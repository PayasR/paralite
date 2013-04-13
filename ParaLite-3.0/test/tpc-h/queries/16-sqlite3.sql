-- $ID$
-- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
-- Functional Query Definition
-- Approved February 1998
select
	P.brand,
	P.type,
	P.size,
	count(distinct PS.suppkey) as supplier_cnt
from
	partsupp PS,
	part P
where
	P.partkey = PS.partkey
	and P.brand <> 'Brand#42'
	and P.type not like 'SMALL%'
	and P.size in ('PROMO BURNISHED COPPER', 'LARGE BRUSHED BRASS', 'STANDARD POLISHED BRASS', 'SMALL PLATED BRASS', 'STANDARD POLISHED TIN', 'PROMO PLATED STEEL', 'SMALL PLATED COPPER', 'PROMO BURNISHED TIN')
	and PS.suppkey not in (
		select
			S.suppkey
		from
			supplier S
		where
			S.comment like '%Customer%Complaints%'
	)
group by
	P.brand,
	P.type,
	P.size
order by
	supplier_cnt desc,
	P.brand,
	P.type,
	P.size
limit 100;
