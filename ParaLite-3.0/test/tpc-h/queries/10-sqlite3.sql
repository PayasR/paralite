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
	Customer C,
	O_Orders O,
	LineItem L,
	Nation N
where
	C.c_custkey = O.o_custkey
	and L.l_orderkey = O.o_orderkey
	and O.o_orderdate >= date('1995-10-11')
	and O.o_orderdate < date('1995-10-11', '+3 month')
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
