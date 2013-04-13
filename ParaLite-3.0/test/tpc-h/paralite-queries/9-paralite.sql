select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			N.n_name as nation,
			O.o_orderdate as o_year,
			L.l_extendedprice as amount
		from
			Part P,
			Supplier S,
			LineItem L,
			PartSupp PS,
			Orders O,
			Nation N
		where
			S.s_suppkey = L.l_suppkey
			and PS.ps_suppkey = L.l_suppkey
			and PS.ps_partkey = L.l_partkey
			and P.p_partkey = L.l_partkey
			and O.o_orderkey = L.l_orderkey
			and S.s_nationkey = N.n_nationkey
			and P.p_name like '%black%'
	) as profit
group by
      	nation,
	o_year
order by
	nation,
	o_year desc
limit 100
;
