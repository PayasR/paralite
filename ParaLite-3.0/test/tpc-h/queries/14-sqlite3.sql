-- $ID$
-- TPC-H/TPC-R Promotion Effect Query (Q14)
-- Functional Query Definition
-- Approved February 1998
select
	100.00 * sum(case
		when P.type like 'PROMO%'
			then L.extendedprice * (1 - L.discount)
		else 0
	end) / sum(L.extendedprice * (1 - L.discount)) as promo_revenue
from
	lineitem L,
	part P
where
	L.partkey = P.partkey
	and L.shipdate >= date('1995-10-11')
	and L.shipdate < date('1995-10-11', '+1 month')
limit 100;
