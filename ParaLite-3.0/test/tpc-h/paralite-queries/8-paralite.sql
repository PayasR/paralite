-- Unsupported!

select  
        o_year,
	sum(volume) as mkt_share
from
        (
                select  
		        O.o_orderdate as o_year, 
                        L.l_extendedprice * (1 - L.l_discount) as volume,
                        n2.n_name as nation
                from
                        Part P,
                        Supplier S,
                        LineItem L,
			Orders O,
                        Customer C,
                        Nation n1,
                        Nation n2,
                        Region R
                where   
                        P.p_partkey = L.l_partkey
                        and S.s_suppkey = L.l_suppkey
                        and L.l_orderkey = O.o_orderkey
                        and O.o_custkey = C.c_custkey
                        and C.c_nationkey = n1.n_nationkey
                        and n1.n_regionkey = R.r_regionkey
                        and R.r_name = 'AMERICA'
                        and S.s_nationkey = n2.n_nationkey
                        and O.o_orderdate between date('1995-01-01') and date('1996-12-31')
                        and P.p_type = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
group by
        o_year
order by
        o_year
limit 100;