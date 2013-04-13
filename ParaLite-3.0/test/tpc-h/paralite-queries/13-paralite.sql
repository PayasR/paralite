select
	c_count,
 	count(*) as custdist
from
 	(
		select
			C.c_custkey as c_custkey
			,
			count(O.o_orderkey) as c_count
		from
			Customer C join Orders O on
				C.c_custkey = O.o_custkey
				and O.o_comment not like '%regular%acro%'
		group by
			C.c_custkey
 	) as c_orders 
group by
        c_count
order by
	custdist desc,
 	c_count desc
limit 100
;

