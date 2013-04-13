select
	O.o_orderpriority,
	count(*) as order_count
from
	O_Orders O
where
	O.o_orderdate >= date('1995-10-11', '-3 month')
	and O.o_orderdate < date('1995-10-11', '+3 month')
	and exists (
		select
			*
		from
			LineItem L
		where
			L.l_orderkey = O.o_orderkey
			and L.l_commitdate < L.l_receiptdate
	)
group by
	O.o_orderpriority
order by
	O.o_orderpriority
limit 100;
