-- $ID$
-- TPC-H/TPC-R Global Sales Opportunity Query (Q22)
-- Functional Query Definition
-- Approved February 1998
select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	(
		select
			substr(C.phone, 1, 2) as cntrycode,
			C.acctbal as c_acctbal
		from
			customer C
		where
			substr(C.phone, 1, 2) in
				('25', '23', '11', '14', '13', '30', '28')
			and C.acctbal > (
				select
					avg(C.acctbal)
				from
					customer
				where
					C.acctbal > 0.00
					and substr(C.phone, 1, 2) in
						('25', '23', '11', '14', '13', '30', '28')
			)
			and not exists (
				select
					*
				from
					orders O
				where
					O.custkey = C.custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode
limit 100;
