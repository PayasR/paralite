-- $ID$
-- TPC-H/TPC-R Product Type Profit Measure Query (Q9)
-- Functional Query Definition
-- Approved February 1998
select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			N.name as nation,
			strftime('%Y', O.orderdate) as o_year,
			L.extendedprice * (1 - L.discount) - PS.supplycost * L.quantity as amount
		from
			part P,
			supplier S,
			lineitem L,
			partsupp PS,
			orders O,
			nation N
		where
			S.suppkey = L.suppkey
			and PS.suppkey = L.suppkey
			and PS.partkey = L.partkey
			and P.partkey = L.partkey
			and O.orderkey = L.orderkey
			and S.nationkey = N.nationkey
			and P.name like '%black%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc
limit 100;
