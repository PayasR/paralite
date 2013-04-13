-- $ID$
-- TPC-H/TPC-R Suppliers Who Kept Orders Waiting Query (Q21)
-- Functional Query Definition
-- Approved February 1998
select
	S.name,
	count(*) as numwait
from
	supplier S,
	lineitem l1,
	orders O,
	nation N
where
	S.suppkey = l1.suppkey
	and O.orderkey = l1.orderkey
	and O.orderstatus = 'F'
	and l1.receiptdate > l1.commitdate
	and exists (
		select
			*
		from
			lineitem l2
		where
			l2.orderkey = l1.orderkey
			and l2.suppkey <> l1.suppkey
	)
	and not exists (
		select
			*
		from
			lineitem l3
		where
			l3.orderkey = l1.orderkey
			and l3.suppkey <> l1.suppkey
			and l3.receiptdate > l3.commitdate
	)
	and S.nationkey = N.nationkey
	and N.name = 'CANADA'
group by
	S.name
order by
	numwait desc,
	S.name
limit 100;
