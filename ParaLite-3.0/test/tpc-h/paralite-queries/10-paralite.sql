select
	C.c_custkey,
	C.c_name,
	sum(L.l_extendedprice * (1 - L.l_discount)) as revenue,
	C.c_acctbal,
	N.n_name,
	C.c_address,
	C.c_phone,
	C.c_comment
from
	Customer C,
	Orders O,
	LineItem L,
	Nation N
where
	C.c_custkey = O.o_custkey
	and L.l_orderkey = O.o_orderkey
	and O.o_orderdate >= date('1993-10-01')
	and O.o_orderdate < date('1994-01-01', '+3 month')
	and L.l_returnflag = 'R'
	and C.c_nationkey = N.n_nationkey
group by
	C.c_custkey,
	C.c_name,
	C.c_acctbal,
	C.c_phone,
	N.n_name,
	C.c_address,
	C.c_comment
order by
	revenue desc
limit 20;
