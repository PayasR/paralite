explain query plan
select
	S.s_acctbal,
	S.s_name,
	N.n_name,
	P.p_partkey,
	P.p_mfgr,
	S.s_address,
	S.s_phone,
	S.s_comment
from
	Part P,
	Supplier S,
	PartSupp PS,
	Nation N,
	Region R
where
	P.p_partkey = PS.ps_partkey
	and S.s_suppkey = PS.ps_suppkey
	and P.p_size = 7
	and P.p_type like '%LARGE%'
	and S.s_nationkey = N.n_nationkey
	and N.n_regionkey = R.r_regionkey
	and R.r_name = 'ASIA'
	and PS.ps_supplycost = (
		select
			min(PS.ps_supplycost)
		from
			PartSupp,
			Supplier,
			Nation,
			Region
		where
			P.p_partkey = PS.ps_partkey
			and S.s_suppkey = PS.ps_suppkey
			and S.s_nationkey = N.n_nationkey
			and N.n_regionkey = R.r_regionkey
			and R.r_name = 'ASIA'
	)
order by
	S.s_acctbal desc,
	N.n_name,
	S.s_name,
	P.p_partkey
limit 100;
