select
	L.l_orderkey,
	sum(L.l_extendedprice * (1 - L.l_discount)) as revenue,
	O.o_orderdate,
	O.o_shippriority
from
	Customer C,
	O_Orders O,
	LineItem L
where
	C.c_mktsegment = 'AUTOMOBILE'
	and C.c_custkey = O.o_custkey
	and L.l_orderkey = O.o_orderkey
	and O.o_orderdate < '1995-10-11'
	and L.l_shipdate > '1995-10-11'
group by
	L.l_orderkey,
	O.o_orderdate,
	O.o_shippriority
order by
	revenue desc,
	O.o_orderdate
limit 10;
