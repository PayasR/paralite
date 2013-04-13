-- $ID$
-- TPC-H/TPC-R Discounted Revenue Query (Q19)
-- Functional Query Definition
-- Approved February 1998
select
	sum(L.extendedprice* (1 - L.discount)) as revenue
from
	lineitem L
join Part P on P.partkey = L.partkey
where
	(
		P.brand = ':1'
		and P.container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and L.quantity >= 7 and L.quantity <= 7 + 10
		and P.size between 1 and 5
		and L.shipmode in ('AIR', 'AIR REG')
		and L.shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		P.brand = ':2'
		and P.container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and L.quantity >= 10 and L.quantity <= 10 + 10
		and P.size between 1 and 10
		and L.shipmode in ('AIR', 'AIR REG')
		and L.shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		P.brand = ':3'
		and P.container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and L.quantity >= 13 and L.quantity <= 13 + 10
		and P.size between 1 and 15
		and L.shipmode in ('AIR', 'AIR REG')
		and L.shipinstruct = 'DELIVER IN PERSON'
	)
limit 100;
