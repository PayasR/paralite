select
        100.00 * sum(L.l_extendedprice * (1 - L.l_discount))
from
        LineItem L,
	Part P
where
        L.l_partkey = P.p_partkey
        and L.l_shipdate >= date('1995-10-11')
        and L.l_shipdate < date('1995-10-11', '+1 month')
limit 100
;
