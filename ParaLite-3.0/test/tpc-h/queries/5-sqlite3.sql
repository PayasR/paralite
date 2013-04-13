select
	N.name,
	sum(L.extendedprice * (1 - L.discount)) as revenue
from
	Customer C,
	O_Orders O,
	LineItem L,
	Supplier S,
	Nation N,
	Region R
where
	C.c_custkey = O.o_custkey
	and L.l_orderkey = O.o_orderkey
	and L.l_suppkey = S.s_suppkey
	and C.c_nationkey = S.s_nationkey
	and S.s_nationkey = N.n_nationkey
	and N.n_regionkey = R.r_regionkey
	and R.r_name = 'EUROPE'
	and O.o_orderdate >= date('1995-10-11')
	and O.o_orderdate < date('1995-10-11', '+1 year')
group by
	N.n_name
order by
	revenue desc
limit 100;
