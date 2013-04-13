-- $ID$
-- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	Part,
	Supplier,
	PartSupp,
	Nation,
	Region
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 1
	and p_type = "aaa"
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = "3"
	and ps_supplycost in (
		select
			ps_supplycost
		from
			PartSupp,
			Supplier,
			Nation,
			Region
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = "2"
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey;
:n 100
