-- $ID$
-- TPC-H/TPC-R Returned Item Reporting Query (Q10)
-- Functional Query Definition
-- Approved February 1998
select
	C.custkey,
	C.name,
	sum(L.extendedprice * (1 - L.discount)) as revenue,
	C.acctbal,
	N.name,
	C.address,
	C.phone,
	C.comment
from
	customer C,
	orders O,
	lineitem L,
	nation N
where
	C.custkey = O.custkey
	and L.orderkey = O.orderkey
	and O.orderdate >= date('1995-10-11')
	and O.orderdate < date_add('1995-10-11', interval 3 month)
	and L.returnflag = 'R'
	and C.nationkey = N.nationkey
group by
	C.custkey,
	C.name,
	C.acctbal,
	C.phone,
	N.name,
	C.address,
	C.comment
order by
	revenue desc
limit 20;
