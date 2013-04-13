-- $ID$
-- TPC-H/TPC-R Large Volume Customer Query (Q18)
-- Function Query Definition
-- Approved February 1998
select
	C.name,
	C.custkey,
	O.orderkey,
	O.orderdate,
	O.totalprice,
	sum(L.quantity)
from
	customer C,
	orders O,
	lineitem L
where
	O.orderkey in (
		select
			L.orderkey
		from
			lineitem L
		group by
			L.orderkey having
				sum(L.quantity) > 7
	)
	and C.custkey = O.custkey
	and O.orderkey = L.orderkey
group by
	C.name,
	C.custkey,
	O.orderkey,
	O.orderdate,
	O.totalprice
order by
	O.totalprice desc,
	O.orderdate
limit 100;
