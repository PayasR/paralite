-- $ID$
-- TPC-H/TPC-R Local Supplier Volume Query (Q5)
-- Functional Query Definition
-- Approved February 1998
select
	N.name,
	sum(L.extendedprice * (1 - L.discount)) as revenue
from
	region R,
	nation N,
	supplier S,
	customer C,
	orders O,
	lineitem L
where
	C.custkey = O.custkey
	and L.orderkey = O.orderkey
	and L.suppkey = S.suppkey
	and C.nationkey = S.nationkey
	and S.nationkey = N.nationkey
	and N.regionkey = R.regionkey
	and R.name = 'EUROPE'
	and O.orderdate >= date('1995-10-11')
	and O.orderdate < date('1995-10-11', '+1 year')
group by
	N.name
order by
	revenue desc
limit 100;
