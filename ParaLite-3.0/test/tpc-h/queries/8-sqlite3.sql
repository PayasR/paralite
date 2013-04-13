-- $ID$
-- TPC-H/TPC-R National Market Share Query (Q8)
-- Functional Query Definition
-- Approved February 1998
select
	o_year,
	sum(case
		when nation = 'CANADA' then volume
		else 0
	end) / sum(volume) as mkt_share
from
	(
		select
			strftime('%Y', O.orderdate) as o_year,
			L.extendedprice * (1 - L.discount) as volume,
			n2.name as nation
		from
			part P,
			supplier S,
			lineitem L,
			orders O,
			customer C,
			nation n1,
			nation n2,
			region R
		where
			P.partkey = L.partkey
			and S.suppkey = L.suppkey
			and L.orderkey = O.orderkey
			and O.custkey = C.custkey
			and C.nationkey = n1.nationkey
			and n1.regionkey = R.regionkey
			and R.name = 'EUROPE'
			and S.nationkey = n2.nationkey
			and O.orderdate between date('1995-01-01') and date('1996-12-31')
			and P.type = 'SMALL PLATED BRASS'
	) as all_nations
group by
	o_year
order by
	o_year
limit 100;
