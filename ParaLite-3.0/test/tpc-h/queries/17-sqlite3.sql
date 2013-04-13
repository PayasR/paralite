-- $ID$
-- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
-- Functional Query Definition
-- Approved February 1998
select
	sum(L.extendedprice) / 7.0 as avg_yearly
from
	lineitem L,
	part P
where
	P.partkey = L.partkey
	and P.brand = 'Brand#13'
	and P.container = 'JUMBO PKG'
	and L.quantity < (
		select
			0.2 * avg(L.quantity)
		from
			lineitem
		where
			L.partkey = P.partkey
	)
limit 100;
