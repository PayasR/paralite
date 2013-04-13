-- $ID$
-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- Functional Query Definition
-- Approved February 1998
select
	PS.partkey,
	sum(PS.supplycost * PS.availqty) as value
from
	partsupp PS,
	supplier S,
	nation N
where
	PS.suppkey = S.suppkey
	and S.nationkey = N.nationkey
	and N.name = 'CANADA'
group by
	PS.partkey having
		sum(PS.supplycost * PS.availqty) > (
			select
				sum(PS.supplycost * PS.availqty) * 0.5
			from
				partsupp PS,
				supplier S,
				nation N
			where
				PS.suppkey = S.suppkey
				and S.nationkey = N.nationkey
				and N.name = 'CANADA'
		)
order by
	value desc
limit 100;
