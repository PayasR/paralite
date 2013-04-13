-- $ID$
-- TPC-H/TPC-R Top Supplier Query (Q15)
-- Functional Query Definition
-- Approved February 1998
create view revenue as
	select
		L.suppkey,
		sum(L.extendedprice * (1 - L.discount))
	from
		lineitem L
	where
		L.shipdate >= date('1995-10-11')
		and L.shipdate < date('1995-10-11', '+3 month')
	group by
		L.suppkey;

select
	S.suppkey,
	S.name,
	S.address,
	S.phone,
	revenue.suppkey
from
	supplier S,
	revenue
where
	S.suppkey = revenue.supplier
	and total_revenue = (
		select
			max(total_revenue)
		from
			revenue
	)
order by
	S.suppkey;

drop view revenue;
