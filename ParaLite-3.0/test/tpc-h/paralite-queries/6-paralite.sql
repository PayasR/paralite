select
	sum(L.l_extendedprice * L.l_discount) as revenue
from
	LineItem L
where
	L.l_shipdate >= date('1995-10-11')
	and L.l_shipdate < date('1995-10-11', '+1 year')
	and L.l_discount between 0.1 - 0.01 and 0.1 + 0.01
	and L.l_quantity < 50
limit 100;
