-- $ID$
-- TPC-H/TPC-R Potential Part Promotion Query (Q20)
-- Function Query Definition
-- Approved February 1998
select
	S.name,
	S.address
from
	supplier S,
	nation N
where
	S.suppkey in (
		select
			PS.suppkey
		from
			partsupp PS
		where
			PS.partkey in (
				select
					P.partkey
				from
					part P
				where
					P.name like 'orange%'
			)
			and PS.availqty > (
				select
					0.5 * sum(L.quantity)
				from
					lineitem L
				where
					L.partkey = PS.partkey
					and L.suppkey = PS.suppkey
					and L.shipdate >= date('1995-10-11')
					and L.shipdate < date('1995-10-11', '+1 year')
			)
	)
	and S.nationkey = N.nationkey
	and N.name = 'CANADA'
order by
	S.name
limit 100;
