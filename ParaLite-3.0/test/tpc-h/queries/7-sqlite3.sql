select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.name as supp_nation,
			n2.name as cust_nation,
			strftime('%Y', L.shipdate) as l_year,
			L.extendedprice * (1 - L.discount) as volume
		from
			supplier S,
			lineitem L,
			orders O,
			customer C,
			nation n1,
			nation n2
		where
			S.suppkey = L.suppkey
			and O.orderkey = L.orderkey
			and C.custkey = O.custkey
			and S.nationkey = n1.nationkey
			and C.nationkey = n2.nationkey
			and (
				(n1.name = 'CANADA' and n2.name = 'FRANCE')
				or (n1.name = 'FRANCE' and n2.name = 'CANADA')
			)
			and L.shipdate between date('1995-01-01') and date('1996-12-31')
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year
limit 100;
